package org.example.lambda;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.example.config.AppConfig;
import org.example.utils.BenchmarkLogger;
import org.example.utils.KafkaReplayProducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LambdaOrchestrator {
    public static void main(String[] args) {
        System.out.println("[SYSTEM] [LAMBDA] Инициализация Lambda-архитектуры...");

        try (Connection conn = DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS lambda_batch (user_id BIGINT, total_actions BIGINT, total_spend DOUBLE PRECISION)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS lambda_speed (user_id BIGINT, total_actions BIGINT, total_spend DOUBLE PRECISION)");

            String createViewSql = "CREATE OR REPLACE VIEW lambda_serving AS " +
                    "SELECT " +
                    "    COALESCE(b.user_id, s.user_id) as user_id, " +
                    "    (COALESCE(b.total_actions, 0) + COALESCE(s.total_actions, 0)) as total_actions, " +
                    "    (COALESCE(b.total_spend, 0.0) + COALESCE(s.total_spend, 0.0)) as total_spend " +
                    "FROM lambda_batch b " +
                    "FULL OUTER JOIN lambda_speed s ON b.user_id = s.user_id";
            stmt.executeUpdate(createViewSql);
            System.out.println("[SYSTEM] [LAMBDA] БД подготовлена");
        } catch (Exception e) {
            System.err.println("[ERROR] [LAMBDA] Сбой подготовки БД: " + e.getMessage());
        }

        new Thread(() -> {
            System.out.println("[SYSTEM] [KAFKA] Запуск симулятора потока...");
            KafkaReplayProducer.main(null);
        }).start();

        SparkSession spark = SparkSession.builder()
                .appName("LambdaOrchestratedSystem")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "5")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);

        try {
            System.out.println("[SYSTEM] [LAMBDA] Запуск стриминг-слоя ...");
            StreamingQuery speedQuery = LambdaSpeedLayer.run(spark);

            ScheduledExecutorService batchScheduler = Executors.newSingleThreadScheduledExecutor();
            batchScheduler.scheduleAtFixedRate(() -> {
                System.out.println("\n[SYSTEM] [LAMBDA BATCH] Запуск перерасчета истории...");
                try {
                    LambdaBatchLayer.run(spark, AppConfig.getHistoryPath());
                } catch (Exception e) {
                    System.err.println("[ERROR] [LAMBDA BATCH] Перерасчет не удался: " + e.getMessage());
                }
            }, 5, 30, TimeUnit.SECONDS);

            ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();
            queryScheduler.scheduleAtFixedRate(() -> {
                long queryStart = System.nanoTime();
                try (Connection conn = DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
                     Statement stmt = conn.createStatement()) {

                    String sql = "SELECT COUNT(*), SUM(total_spend) FROM lambda_serving WHERE total_spend > 100";
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            double queryDurationMs = (System.nanoTime() - queryStart) / 1_000_000.0;
                            System.out.printf("[METRIC] [LAMBDA SERVING] Активные пользователи: %d | Выручка: %.2f | Задержка БД: %.2f мс%n",
                                    rs.getLong(1), rs.getDouble(2), queryDurationMs);
                            BenchmarkLogger.log("Lambda", "ServingQueryLatency", queryDurationMs);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] [LAMBDA SERVING] Сбой аналитического запроса: " + e.getMessage());
                }
            }, 10, 10, TimeUnit.SECONDS);

            speedQuery.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}