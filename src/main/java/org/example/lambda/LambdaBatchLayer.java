package org.example.lambda;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.example.config.AppConfig;
import org.example.utils.BenchmarkLogger;

import static org.apache.spark.sql.functions.*;

public class LambdaBatchLayer {
    public static void run(SparkSession spark, String historyPath) {
        SparkSession.setActiveSession(spark);
        long startTime = System.currentTimeMillis();

        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(historyPath);

        Dataset<Row> aggregatedData = rawData.groupBy("user_id")
                .agg(
                        count("event_type").as("total_actions"),
                        sum("price").as("total_spend")
                );

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
             java.sql.Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE lambda_batch");
        } catch (Exception e) {
        }

        aggregatedData.write()
                .format("jdbc")
                .option("url", AppConfig.JDBC_URL)
                .option("dbtable", "lambda_batch")
                .option("user", AppConfig.DB_USER)
                .option("password", AppConfig.DB_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .mode(SaveMode.Append)
                .save();

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("[METRIC] [LAMBDA BATCH] Пакетный расчет истории завершен: %.2f сек%n", duration / 1000.0);
        BenchmarkLogger.log("Lambda", "BatchExecution", duration);
    }
}