#!/bin/bash

# Папка с Kafka
KAFKA_DIR=~/repos/StockAI/kafka_2.13-3.9.0

# Функция для остановки всех процессов Kafka и очистки блокировок
stop_kafka() {
    echo "⏹ Остановка Kafka..."
    sudo pkill -9 -f kafka
    sleep 3  # Ожидание завершения процессов
    echo "✅ Все процессы Kafka остановлены."
    echo "🧹 Очистка блокировок..."
    rm -rf /tmp/kafka-logs/.lock
    rm -rf /tmp/kafka-logs
    echo "✅ Блокировки удалены."
}

# Функция для запуска Zookeeper и Kafka
start_kafka() {
    echo "🚀 Запуск Zookeeper..."
    $KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
    sleep 5  # Ожидание перед запуском Kafka
    echo "🚀 Запуск Kafka..."
    $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
    echo "✅ Kafka запущена!"
}

# Функция для проверки работы Kafka
check_kafka() {
    echo "🔍 Проверка запущенных процессов Kafka..."
    ps aux | grep kafka | grep -v grep
}

# Функция для проверки сообщений в Kafka-топике
check_kafka_market_data() {
    echo "📡 Чтение сообщений из Kafka-топика market-data..."
    $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic market-data --from-beginning --timeout-ms 5000
}

check_kafka_historical_data() {
    echo "📡 Чтение сообщений из Kafka-топика market-data..."
    $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic historical-data --from-beginning --timeout-ms 5000
}

# Меню команд
case "$1" in
    start)
        start_kafka
        ;;
    stop)
        stop_kafka
        ;;
    restart)
        stop_kafka
        sleep 3
        start_kafka
        ;;
    status)
        check_kafka
        ;;
    check-data-market)
        check_kafka_market_data
        ;;
    check-data-historical)
        check_kafka_historical_data
        ;;
    *)
        echo "Использование: $0 {start|stop|restart|status|check-data}"
        exit 1
        ;;
esac

exit 0