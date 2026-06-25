package org.example.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CsvSplitter {
    public static void main(String[] args) {
        double targetSizeMB = 3 * 1024;

        String sourcePath = "data/2019-Oct.csv";
        String historyPath = "data/history-oct.csv";
        String streamPath = "data/stream-oct.csv";

        if (targetSizeMB > 0) {
            String suffix = "_" + (int) targetSizeMB + "M";
            historyPath = "data/history-oct" + suffix + ".csv";
            streamPath = "data/stream-oct" + suffix + ".csv";
        }

        double splitRatio = 0.8;

        try {
            System.out.println("Начало работы с файлом: " + sourcePath);
            long startTime = System.currentTimeMillis();

            long targetSizeBytes;
            if (targetSizeMB > 0) {
                targetSizeBytes = (long) (targetSizeMB * 1024 * 1024);
                System.out.println("Целевой размер нарезки: " + targetSizeMB + " МБ");
            } else {
                targetSizeBytes = Files.size(Paths.get(sourcePath));
                System.out.println("Обрабатываем весь файл целиком. Общий размер: " + (targetSizeBytes / (1024.0 * 1024.0)) + " МБ");
            }

            long historyLimitBytes = (long) (targetSizeBytes * splitRatio);

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourcePath));
                 BufferedWriter historyWriter = Files.newBufferedWriter(Paths.get(historyPath));
                 BufferedWriter streamWriter = Files.newBufferedWriter(Paths.get(streamPath))) {

                String header = reader.readLine();
                if (header == null) {
                    System.err.println("Файл пуст.");
                    return;
                }
                historyWriter.write(header);
                historyWriter.newLine();
                streamWriter.write(header);
                streamWriter.newLine();

                String line;
                long processedBytes = 0;
                long linesCount = 0;

                while ((line = reader.readLine()) != null) {
                    long lineBytes = line.length() + 1;
                    processedBytes += lineBytes;
                    linesCount++;

                    if (processedBytes > targetSizeBytes) {
                        break;
                    }

                    if (processedBytes <= historyLimitBytes) {
                        historyWriter.write(line);
                        historyWriter.newLine();
                    } else {
                        streamWriter.write(line);
                        streamWriter.newLine();
                    }

                    if (linesCount % 1000000 == 0) {
                        System.out.printf("Обработано строк: %d (~%.2f МБ)%n",
                                linesCount, processedBytes / (1024.0 * 1024.0));
                    }
                }
            }

            long duration = (System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Разделение успешно завершено за " + duration + " сек.!");

        } catch (IOException e) {
            System.err.println("Ошибка при работе с файлами: " + e.getMessage());
            e.printStackTrace();
        }
    }
}