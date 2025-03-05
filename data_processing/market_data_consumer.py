from confluent_kafka import Consumer, KafkaError
import json

# Конфигурация Kafka Consumer
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "market-data"

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'market-data-group',
    'auto.offset.reset': 'earliest'  # Читаем сообщения с начала, если группа новая
})

# Подписка на топик
consumer.subscribe([TOPIC_NAME])

print(f"📡 Ожидание данных из Kafka-топика: {TOPIC_NAME}...")

try:
    while True:
        msg = consumer.poll(1.0)  # Ожидание нового сообщения (1 секунда)

        if msg is None:
            continue  # Если нет сообщений, ждём дальше

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"📌 Достигнут конец раздела {msg.topic()} [{msg.partition()}] при {msg.offset()}")
            else:
                print(f"❌ Ошибка при чтении сообщения: {msg.error()}")
            continue

        # Декодируем JSON-сообщение
        try:
            stock_data = json.loads(msg.value().decode('utf-8'))
            print(f"✅ Получены данные: {stock_data}")
        except json.JSONDecodeError:
            print(f"⚠ Ошибка декодирования JSON: {msg.value()}")

except KeyboardInterrupt:
    print("⏹ Остановка Consumer'а...")
finally:
    consumer.close()
