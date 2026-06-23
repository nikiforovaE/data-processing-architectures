package org.example.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CsvSplitter {
    public static void main(String[] args) {

        String sourcePath = "data/2019-Oct.csv";
        String historyPath = "data/history-oct.csv";
        String streamPath = "data/stream-oct.csv";

        double splitRatio = 0.8;

        try {
            System.out.println("Начало работы с файлом: " + sourcePath);
            long startTime = System.currentTimeMillis();

            long totalLines = 0;
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourcePath))) {
                while (reader.readLine() != null) {
                    totalLines++;
                }
            }

            if (totalLines <= 1) {
                System.err.println("Файл пуст или содержит только заголовок.");
                return;
            }

            long dataLines = totalLines - 1;
            long historyLimit = (long) (dataLines * splitRatio);

            System.out.println("Всего строк данных: " + dataLines);
            System.out.println("Будет записано в историю: " + historyLimit);
            System.out.println("Будет записано в поток: " + (dataLines - historyLimit));

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourcePath));
                 BufferedWriter historyWriter = Files.newBufferedWriter(Paths.get(historyPath));
                 BufferedWriter streamWriter = Files.newBufferedWriter(Paths.get(streamPath))) {

                String header = reader.readLine();
                historyWriter.write(header);
                historyWriter.newLine();
                streamWriter.write(header);
                streamWriter.newLine();

                String line;
                long currentLineCount = 0;

                while ((line = reader.readLine()) != null) {
                    currentLineCount++;
                    if (currentLineCount <= historyLimit) {
                        historyWriter.write(line);
                        historyWriter.newLine();
                    } else {
                        streamWriter.write(line);
                        streamWriter.newLine();
                    }

                    if (currentLineCount % 1000000 == 0) {
                        System.out.println("Обработано строк: " + currentLineCount);
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("Ошибка при работе с файлами: " + e.getMessage());
            e.printStackTrace();
        }
    }
}