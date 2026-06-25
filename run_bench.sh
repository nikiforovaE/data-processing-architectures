#!/bin/bash

mkdir -p data/plots

RESOURCE_LOG="data/plots/resource_stats.csv"
if [ ! -f "$RESOURCE_LOG" ]; then
    echo "Architecture,DatasetSize,Container,CPU,MEM,BlockIO" > "$RESOURCE_LOG"
fi

echo "Какой масштаб данных? (Small / Medium-1 / Medium-2 / Large):"
read -r DATASET_SIZE

echo "Сколько секунд тестировать?"
read -r TIME

ARCHITECTURES=("lambda" "kappa" "lakehouse")

start_resource_logger() {
    local arch=$1
    local size=$2
    echo "=== Запуск мониторинга ресурсов для $arch ($size) ==="

    (
        while true; do
            docker stats --no-stream --format "$arch,$size,{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.BlockIO}}" | grep -E "spark|kafka|serving_db" >> "$RESOURCE_LOG"
            sleep 2
        done
    ) &
    LOGGER_PID=$!
}

stop_resource_logger() {
    if [ -n "$LOGGER_PID" ]; then
        kill "$LOGGER_PID" 2>/dev/null
        wait "$LOGGER_PID" 2>/dev/null
        LOGGER_PID=""
        echo "=== Мониторинг ресурсов остановлен ==="
    fi
}

kill_java_processes() {
    echo "Принудительная очистка фоновых процессов Java и Maven..."
    pkill -f "org.example.lambda" 2>/dev/null
    pkill -f "org.example.kappa" 2>/dev/null
    pkill -f "org.example.lakehouse" 2>/dev/null
    pkill -f "exec:java" 2>/dev/null
}

cleanup_and_exit() {
    stop_resource_logger
    kill_java_processes
    docker compose down -v
    exit 1
}

trap cleanup_and_exit SIGINT

for ARCH in "${ARCHITECTURES[@]}"; do
    echo "=========================================================="
    echo "НАЧАЛО АВТОМАТИЧЕСКОГО ТЕСТА АРХИТЕКТУРЫ: $ARCH"
    echo "=========================================================="

    docker compose up -d

    echo "Ожидание 10 секунд для полной готовности систем..."
    sleep 10

    start_resource_logger "$ARCH" "$DATASET_SIZE_LOOP"

    echo "Запуск оркестратора $ARCH..."

    RUN_CMD="mvn compile exec:java -Dexec.mainClass"

    if [ "$ARCH" == "lambda" ]; then
        timeout "$TIME"s $RUN_CMD="org.example.lambda.LambdaOrchestrator"
    elif [ "$ARCH" == "kappa" ]; then
        timeout "$TIME"s $RUN_CMD="org.example.kappa.KappaOrchestrator"
    elif [ "$ARCH" == "lakehouse" ]; then
        timeout "$TIME"s $RUN_CMD="org.example.lakehouse.LakehouseOrchestrator"
    fi

    stop_resource_logger

    kill "$!" 2>/dev/null
    killall java 2>/dev/null
    pkill -f "exec:java"
    echo "Служебные Java-процессы остановлены."

    echo "Сброс контейнеров и очистка томов Docker..."
    docker compose down -v
    echo "Тест архитектуры $ARCH успешно завершен."
    echo "----------------------------------------------------------"
    sleep 5
done

echo "=========================================================="
echo "Все тесты успешно завершены!"
echo "Метрики производительности сохранены в: data/benchmark_results.csv"
echo "Метрики потребления ресурсов сохранены в: $RESOURCE_LOG"
echo "=========================================================="