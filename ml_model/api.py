from fastapi import FastAPI
import torch
import json
from kafka import KafkaProducer
from ml_model.predict import predict, load_model, count_parameters, get_latest_data, prepare_input, send_prediction
import uvicorn

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"
OUTPUT_TOPIC = "predictions"

# Инициализация FastAPI
app = FastAPI()

# Загрузка модели при запуске API
model = load_model()

@app.get("/")
def read_root():
    return {"message": "🚀 FastAPI работает! Используй /predict для прогноза."}

@app.get("/predict/")
def get_prediction():
    try:
        df = get_latest_data()
        if df is None or df.empty:
            return {"error": "Нет данных в Kafka для предсказания!"}

        X_input, scaler = prepare_input(df)

        with torch.no_grad():
            prediction = model(X_input).item()

        # Декодируем предсказанную цену обратно в реальный масштаб
        prediction = scaler.inverse_transform([[0, 0, 0, prediction, 0]])[0][3]
        symbol = df.iloc[-1]["symbol"]

        # Отправляем предсказание в Kafka
        send_prediction(prediction, symbol)

        return {"symbol": symbol, "predicted_close_price": round(prediction, 2)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/model_info/")
def get_model_info():
    return {"parameters_count": count_parameters(model)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
