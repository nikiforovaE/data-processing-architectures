package org.example.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.example.config.AppConfig;
import static org.apache.spark.sql.functions.*;

public class LakehouseStreamingPipeline {
    public static StreamingQuery run(SparkSession spark) throws Exception {
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", AppConfig.KAFKA_SERVERS)
                .option("subscribe", AppConfig.KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) as csv_line")
                .select(split(col("csv_line"), ",").as("cols"))
                .selectExpr(
                        "cols[0] as event_time",
                        "cols[1] as event_type",
                        "CAST(cols[2] AS LONG) as product_id",
                        "CAST(cols[3] AS LONG) as category_id",
                        "cols[4] as category_code",
                        "cols[5] as brand",
                        "CAST(cols[6] AS DOUBLE) as price",
                        "CAST(cols[7] AS LONG) as user_id",
                        "cols[8] as user_session"
                );

        String deltaTablePath = "data/warehouse/delta_transactions";
        String checkpointPath = "data/warehouse/checkpoints_delta";

        return parsedStream.writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start(deltaTablePath);
    }
}