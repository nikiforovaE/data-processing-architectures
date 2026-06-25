package org.example.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.example.config.AppConfig;
import org.example.utils.BenchmarkLogger;
import org.example.utils.KafkaReplayProducer;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class LakehouseOrchestrator {
    private static final String DELTA_TABLE_PATH = "data/warehouse/delta_transactions";
    private static final String CHECKPOINT_PATH = "data/warehouse/checkpoints_delta";

    public static void main(String[] args) {
        System.out.println("[SYSTEM] [LAKEHOUSE] Запуск Delta-Lakehouse...");

        deleteDirectory(new File(DELTA_TABLE_PATH));
        deleteDirectory(new File(CHECKPOINT_PATH));
        System.out.println("[SYSTEM] [LAKEHOUSE] Директории хранилища очищены.");

        SparkSession spark = SparkSession.builder()
                .appName("LakehouseOrchestratedSystem")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "5")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);

        try {
            System.out.println("[SYSTEM] [LAKEHOUSE] Пакетная загрузка истории...");
            LakehouseBatchLoader.run(spark, AppConfig.getHistoryPath(), DELTA_TABLE_PATH);

            System.out.println("[SYSTEM] [LAKEHOUSE] Запуск стриминга в Delta...");
            StreamingQuery lakehouseQuery = LakehouseStreamingPipeline.run(spark);

            new Thread(() -> {
                System.out.println("[SYSTEM] [KAFKA] Запуск симулятора потока...");
                KafkaReplayProducer.main(null);
            }).start();

            ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();
            queryScheduler.scheduleAtFixedRate(() -> {
                long queryStart = System.nanoTime();
                try {
                    Dataset<Row> deltaDf = spark.read().format("delta").load(DELTA_TABLE_PATH);

                    Dataset<Row> userAgg = deltaDf.groupBy("user_id")
                            .agg(sum("price").as("total_spend"));

                    Dataset<Row> finalMetric = userAgg.filter("total_spend > 100")
                            .agg(count("*"), sum("total_spend"));

                    Row result = finalMetric.first();
                    double queryDurationMs = (System.nanoTime() - queryStart) / 1_000_000.0;

                    long activeUsers = result.isNullAt(0) ? 0 : result.getLong(0);
                    double totalRevenue = result.isNullAt(1) ? 0.0 : result.getDouble(1);

                    System.out.printf("[METRIC] [LAKEHOUSE] Активные пользователи: %d | Выручка: %.2f | Задержка: %.2f мс%n",
                            activeUsers, totalRevenue, queryDurationMs);

                    BenchmarkLogger.log("Lakehouse", "ServingQueryLatency", queryDurationMs);
                } catch (Exception e) {
                    System.err.println("[ERROR] [LAKEHOUSE] Запрос не удался: " + e.getMessage());
                }
            }, 10, 10, TimeUnit.SECONDS);

            lakehouseQuery.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}