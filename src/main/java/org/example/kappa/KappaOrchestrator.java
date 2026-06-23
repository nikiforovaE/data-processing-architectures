package org.example.kappa;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.example.config.AppConfig;
import org.example.utils.BenchmarkLogger;
import org.example.utils.HistoryToKafkaLoader;
import org.example.utils.KafkaReplayProducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KappaOrchestrator {
    private static final String DB_TABLE = "kappa_metrics";

    public static void main(String[] args) {
        System.out.println("[SYSTEM] [KAPPA] Инициализация Kappa...");

        try (Connection conn = DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE " + DB_TABLE);
            System.out.println("[SYSTEM] [KAPPA] Таблица метрик очищена.");
        } catch (Exception e) {
            System.out.println("[SYSTEM] [KAPPA] Таблица метрик будет создана автоматически.");
        }

        System.out.println("[SYSTEM] [KAPPA] Загрузка исторического архива в Kafka...");
        long historyLoadStart = System.currentTimeMillis();
        HistoryToKafkaLoader.main(null);
        long historyLoadDuration = System.currentTimeMillis() - historyLoadStart;
        BenchmarkLogger.log("Kappa", "HistoryLoadToKafka", historyLoadDuration);

        SparkSession spark = SparkSession.builder()
                .appName("KappaOrchestratedSystem")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "5")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);

        try {
            System.out.println("[SYSTEM] [KAPPA] Запуск потокового конвейера...");
            long catchUpStart = System.currentTimeMillis();
            StreamingQuery kappaQuery = KappaStreamingPipeline.run(spark);

            new Thread(() -> {
                try {
                    System.out.println("[SYSTEM] [KAPPA] Ожидание обработки исторических данных из Kafka...");
                    long lastCount = -1;

                    while (true) {
                        Thread.sleep(3000);
                        long currentCount = getDatabaseProcessedRowsCount();

                        System.out.println("[SYSTEM] [KAPPA] Прогресс догонки: " + currentCount + " строк записано в PostgreSQL.");

                        if (currentCount > 0 && currentCount == lastCount) {
                            long catchUpDuration = System.currentTimeMillis() - catchUpStart - 3000;
                            System.out.printf("[METRIC] [KAPPA CATCHUP] Пересчет истории завершен: %.2f сек%n",
                                    catchUpDuration / 1000.0);
                            BenchmarkLogger.log("Kappa", "HistoricalCatchUpStream", catchUpDuration);
                            break;
                        }
                        lastCount = currentCount;
                    }

                    System.out.println("[SYSTEM] [KAFKA PRODUCER] Запуск симулятора потока...");
                    KafkaReplayProducer.main(null);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();
            queryScheduler.scheduleAtFixedRate(() -> {
                long queryStart = System.nanoTime();
                try (Connection conn = DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
                     Statement stmt = conn.createStatement()) {

                    String sql = "SELECT COUNT(*), SUM(total_spend) FROM kappa_metrics WHERE total_spend > 100";
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            double queryDurationMs = (System.nanoTime() - queryStart) / 1_000_000.0;
                            System.out.printf("[METRIC] [KAPPA SERVING] Активные пользователи: %d | Выручка: %.2f | Задержка БД: %.2f мс%n",
                                    rs.getLong(1), rs.getDouble(2), queryDurationMs);
                            BenchmarkLogger.log("Kappa", "ServingQueryLatency", queryDurationMs);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] [KAPPA SERVING] Ошибка выполнения тестового запроса: " + e.getMessage());
                }
            }, 5, 5, TimeUnit.SECONDS);

            kappaQuery.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static long getDatabaseProcessedRowsCount() {
        try (Connection conn = DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SUM(total_actions) FROM " + DB_TABLE)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
        }
        return 0;
    }
}