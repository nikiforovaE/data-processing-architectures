package org.example.config;

public class AppConfig {
    // Центральный переключатель: "Small" (December, 415MB) или "Large" (October, 5.7GB)
    public static final String DATASET_SIZE = "Large";

    public static final String KAFKA_SERVERS = "localhost:9094";
    public static final String KAFKA_TOPIC = "ecommerce-events";
    public static final String JDBC_URL = "jdbc:postgresql://localhost:5432/serving_layer";
    public static final String DB_USER = "postgres";
    public static final String DB_PASSWORD = "postgres";

    public static String getHistoryPath() {
        return DATASET_SIZE.equalsIgnoreCase("Large") ? "data/history-oct.csv" : "data/history-dec.csv";
    }

    public static String getStreamPath() {
        return DATASET_SIZE.equalsIgnoreCase("Large") ? "data/stream-oct.csv" : "data/stream-dec.csv";
    }
}