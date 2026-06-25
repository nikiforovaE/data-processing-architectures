package org.example.config;

public class AppConfig {
    // Центральный переключатель масштаба данных:
    // "Small" (415 MB), "Medium-1" (1.5 GB), "Medium-2" (3.0 GB), "Large" (5.7 GB)
    public static final String DATASET_SIZE = "Medium-1";

    public static final String KAFKA_SERVERS = "localhost:9094";
    public static final String KAFKA_TOPIC = "ecommerce-events";
    public static final String JDBC_URL = "jdbc:postgresql://localhost:5432/serving_layer";
    public static final String DB_USER = "postgres";
    public static final String DB_PASSWORD = "postgres";

    public static String getHistoryPath() {
        switch (DATASET_SIZE.toUpperCase()) {
            case "SMALL":
                return "data/history-dec.csv";
            case "MEDIUM-1":
                return "data/history-oct_1536M.csv";
            case "MEDIUM-2":
                return "data/history-oct_3072M.csv";
            case "LARGE":
            default:
                return "data/history-oct.csv";
        }
    }

    public static String getStreamPath() {
        switch (DATASET_SIZE.toUpperCase()) {
            case "SMALL":
                return "data/stream-dec.csv";
            case "MEDIUM-1":
                return "data/stream-oct_1536M.csv";
            case "MEDIUM-2":
                return "data/stream-oct_3072M.csv";
            case "LARGE":
            default:
                return "data/stream-oct.csv";
        }
    }
}