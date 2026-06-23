package org.example.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class LakehouseBatchLoader {
    public static void run(SparkSession spark, String historyPath, String deltaTablePath) {
        long startTime = System.currentTimeMillis();

        Dataset<Row> historyDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(historyPath);

        historyDf.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .save(deltaTablePath);

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("[METRIC] [LAKEHOUSE BATCH] Исторические данные загружены в Delta Lake: %.2f сек%n",
                duration / 1000.0);
    }
}