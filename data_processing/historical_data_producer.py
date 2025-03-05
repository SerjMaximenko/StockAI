from confluent_kafka import Producer
import yfinance as yf
import json
import time

# Конфигурация Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "historical-data"

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Список акций для загрузки
STOCKS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]

def fetch_and_send_historical_data():
    for symbol in STOCKS:
        try:
            print(f"📡 Загружаем исторические данные для {symbol}...")
            stock = yf.Ticker(symbol)
            data = stock.history(period="10y")  # Загружаем данные за 10 лет

            for timestamp, row in data.iterrows():
                message = {
                    "symbol": symbol,
                    "timestamp": str(timestamp),
                    "open_price": round(row["Open"], 2),
                    "close_price": round(row["Close"], 2),
                    "high_price": round(row["High"], 2),
                    "low_price": round(row["Low"], 2),
                    "volume": int(row["Volume"]),
                }

                producer.produce(TOPIC_NAME, key=symbol, value=json.dumps(message))

            print(f"✅ Исторические данные {symbol} отправлены в Kafka.")

        except Exception as e:
            print(f"❌ Ошибка при загрузке {symbol}: {e}")

    producer.flush()

if __name__ == "__main__":
    fetch_and_send_historical_data()
