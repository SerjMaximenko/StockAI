from confluent_kafka import Producer
import yfinance as yf
import json
import time

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"  # Или IP-адрес VPS, если Kafka не локальная
TOPIC_NAME = "market-data"

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Список акций для мониторинга
STOCKS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]

def fetch_and_send_data():
    while True:
        for symbol in STOCKS:
            try:
                stock = yf.Ticker(symbol)
                data = stock.history(period="1d", interval="1h").iloc[-1]  # Последняя запись

                message = {
                    "symbol": symbol,
                    "timestamp": str(data.name),
                    "open_price": round(data["Open"], 2),
                    "close_price": round(data["Close"], 2),
                    "high_price": round(data["High"], 2),
                    "low_price": round(data["Low"], 2),
                    "volume": int(data["Volume"]),
                }

                producer.produce(TOPIC_NAME, key=symbol, value=json.dumps(message))
                print(f"✅ Отправлено: {message}")

            except Exception as e:
                print(f"❌ Ошибка для {symbol}: {e}")

        producer.flush()
        time.sleep(3600)  # Обновление данных раз в час

if __name__ == "__main__":
    fetch_and_send_data()
