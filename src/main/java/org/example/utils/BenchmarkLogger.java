package org.example.utils;

import org.example.config.AppConfig;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BenchmarkLogger {
    private static final String FILE_PATH = "data/benchmark_results.csv";

    public static synchronized void log(String architecture, String metricName, double valueMs) {
        try {
            boolean isNew = !Files.exists(Paths.get(FILE_PATH));
            try (FileWriter fw = new FileWriter(FILE_PATH, true);
                 PrintWriter pw = new PrintWriter(fw)) {
                if (isNew) {
                    pw.println("DatasetSize,Architecture,MetricName,ValueMs");
                }
                pw.printf("%s,%s,%s,%.2f%n", AppConfig.DATASET_SIZE, architecture, metricName, valueMs);
            }
        } catch (Exception e) {
            System.err.println("[ERROR] [LOGGER] Ошибка записи: " + e.getMessage());
        }
    }
}