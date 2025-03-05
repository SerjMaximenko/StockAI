from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
producer.produce('test-topic', key="test", value="Hello Kafka!")
producer.flush()

print("✅ Сообщение отправлено в Kafka")