import time

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
import json
from sklearn.preprocessing import MinMaxScaler
import os

# Проверяем, доступен ли GPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"🔄 Используем устройство: {device}")

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "historical-data"

# Гиперпараметры модели
SEQUENCE_LENGTH = 50
EPOCHS = 200
BATCH_SIZE = 16
LEARNING_RATE = 0.001

# Создаём Kafka Consumer для загрузки данных
def load_data_from_kafka():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    data = []
    timeout = 10  # Ожидание данных 10 секунд
    start_time = time.time()

    while time.time() - start_time < timeout:
        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for _, record in msg.items():
                for message in record:
                    data.append(message.value)

    consumer.close()

    if not data:
        print("⚠ Нет данных в Kafka! Проверь `historical-data`.")
        exit(1)

    return pd.DataFrame(data)

# Создаём LSTM-модель
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

# Преобразуем данные в формат для LSTM
def prepare_data(df):
    df.sort_values("timestamp", inplace=True)
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[["open_price", "high_price", "low_price", "close_price", "volume"]])

    X, y = [], []
    for i in range(len(scaled_data) - SEQUENCE_LENGTH):
        X.append(scaled_data[i:i + SEQUENCE_LENGTH])
        y.append(scaled_data[i + SEQUENCE_LENGTH, 3])  # Целевая переменная - цена закрытия

    return np.array(X), np.array(y), scaler

# Обучение модели
def train_model():
    print("📡 Загружаем данные из Kafka...")
    df = load_data_from_kafka()

    print("📊 Подготовка данных...")
    X, y, scaler = prepare_data(df)

    X_train = torch.tensor(X, dtype=torch.float32)
    y_train = torch.tensor(y, dtype=torch.float32).view(-1, 1)

    model = StockLSTM(input_size=5, hidden_size=70, num_layers=3, output_size=1)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)

    print("🚀 Начинаем обучение...")
    for epoch in range(EPOCHS):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train)
        loss = criterion(outputs, y_train)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 5 == 0:
            print(f"🟢 Epoch [{epoch+1}/{EPOCHS}], Loss: {loss.item():.6f}")

    print("✅ Обучение завершено! Сохраняем модель...")
    os.makedirs("models", exist_ok=True)
    torch.save(model.state_dict(), "models/stock_lstm.pth")
    print("📁 Модель сохранена в models/stock_lstm.pth")

if __name__ == "__main__":
    train_model()
