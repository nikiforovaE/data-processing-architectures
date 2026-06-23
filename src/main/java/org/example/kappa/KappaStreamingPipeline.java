package org.example.kappa;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.example.config.AppConfig;

import static org.apache.spark.sql.functions.*;

public class KappaStreamingPipeline {
    public static StreamingQuery run(SparkSession spark) throws Exception {
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", AppConfig.KAFKA_SERVERS)
                .option("subscribe", AppConfig.KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) as csv_line")
                .select(split(col("csv_line"), ",").as("cols"))
                .selectExpr(
                        "cols[1] as event_type",
                        "CAST(cols[6] AS DOUBLE) as price",
                        "CAST(cols[7] AS LONG) as user_id"
                );

        Dataset<Row> aggregatedStream = parsedStream.groupBy("user_id")
                .agg(
                        count("event_type").as("total_actions"),
                        sum("price").as("total_spend")
                );

        return aggregatedStream.writeStream()
                .outputMode("complete")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .foreachBatch((Dataset<Row> batchDf, Long batchId) -> {
                    SparkSession.setActiveSession(batchDf.sparkSession());

                    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(AppConfig.JDBC_URL, AppConfig.DB_USER, AppConfig.DB_PASSWORD);
                         java.sql.Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate("TRUNCATE TABLE kappa_metrics");
                    } catch (Exception e) {
                    }

                    batchDf.write()
                            .format("jdbc")
                            .option("url", AppConfig.JDBC_URL)
                            .option("dbtable", "kappa_metrics")
                            .option("user", AppConfig.DB_USER)
                            .option("password", AppConfig.DB_PASSWORD)
                            .option("driver", "org.postgresql.Driver")
                            .mode(SaveMode.Append)
                            .save();
                })
                .start();
    }
}