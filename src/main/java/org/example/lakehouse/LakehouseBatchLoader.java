package org.example.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.utils.BenchmarkLogger;

public class LakehouseBatchLoader {
    public static void run(SparkSession spark, String historyPath, String deltaTablePath) {
        System.out.println("[LAKEHOUSE BATCH] Запуск раздельного замера чтения и записи...");

        long readStart = System.currentTimeMillis();
        Dataset<Row> historyDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(historyPath);

        long rowsCount = historyDf.count();
        long readDuration = System.currentTimeMillis() - readStart;

        BenchmarkLogger.log("Lakehouse", "LakehouseBatchRead", readDuration);

        long writeStart = System.currentTimeMillis();
        historyDf.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .save(deltaTablePath);
        long writeDuration = System.currentTimeMillis() - writeStart;

        BenchmarkLogger.log("Lakehouse", "LakehouseBatchWrite", writeDuration);

        long totalDuration = readDuration + writeDuration;
        BenchmarkLogger.log("Lakehouse", "BatchLoadToDelta", totalDuration);

        System.out.printf("[METRIC] [LAKEHOUSE TOTAL] Завершено! Чтение: %.2f сек, Запись: %.2f сек (Всего: %.2f сек)%n",
                readDuration / 1000.0, writeDuration / 1000.0, totalDuration / 1000.0);
    }
}