import time

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import json
from sklearn.preprocessing import MinMaxScaler

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "market-data"
OUTPUT_TOPIC = "predictions"

SEQUENCE_LENGTH = 50

# Определение модели LSTM
class StockLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(StockLSTM, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

# Загрузка модели
def load_model(model_path="models/stock_lstm.pth"):
    model = StockLSTM(input_size=5, hidden_size=70, num_layers=3, output_size=1)
    model.load_state_dict(torch.load(model_path))
    model.eval()
    return model

# Получение последних данных из Kafka
import time

def get_latest_data():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",  # Читаем ВСЕ сообщения
        enable_auto_commit=True,       # Разрешаем коммитить offset
        group_id="stock-predictor",    # Группируем consumer'ы
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    data = []
    timeout = 10  # Ждём данные 10 секунд
    start_time = time.time()

    while time.time() - start_time < timeout:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for _, record in msg.items():
                for message in record:
                    data.append(message.value)
        if len(data) >= SEQUENCE_LENGTH:
            break

    consumer.close()

    if not data:
        print("⚠ Нет данных в Kafka! Проверь `market-data`.")
        exit(1)

    return pd.DataFrame(data)

# Преобразование данных для модели
def prepare_input(df):
    df.sort_values("timestamp", inplace=True)
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[["open_price", "high_price", "low_price", "close_price", "volume"]])
    X = np.array([scaled_data])
    return torch.tensor(X, dtype=torch.float32), scaler

# Отправка предсказания в Kafka
def send_prediction(prediction, symbol):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    message = {"symbol": symbol, "predicted_close_price": float(prediction)}
    producer.send(OUTPUT_TOPIC, value=message)
    producer.flush()
    print(f"📡 Отправлено предсказание: {message}")

# Основная функция предсказания
def predict():
    print("📡 Загружаем модель...")
    model = load_model()

    print("📡 Получаем данные из Kafka...")
    df = get_latest_data()
    if df is None:
        return

    X_input, scaler = prepare_input(df)

    with torch.no_grad():
        prediction = model(X_input).item()

    # Декодируем предсказанную цену обратно в реальный масштаб
    prediction = scaler.inverse_transform([[0, 0, 0, prediction, 0]])[0][3]
    symbol = df.iloc[-1]["symbol"]

    print(f"✅ Предсказанная цена закрытия для {symbol}: {prediction:.2f}")

    send_prediction(prediction, symbol)

def count_parameters(model):
    return sum(p.numel() for p in model.parameters())

if __name__ == "__main__":
    model = load_model()
    print(f"📊 Количество параметров в модели: {count_parameters(model):,}")
    predict()