package org.example.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.config.AppConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class HistoryToKafkaLoader {
    public static void main(String[] args) {
        String historyFilePath = AppConfig.getHistoryPath();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        System.out.println("[SYSTEM] [KAFKA LOADER] Загрузка истории из " + historyFilePath + " в Kafka...");
        long startTime = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(Paths.get(historyFilePath))) {

            reader.readLine();
            String line;
            long sentCount = 0;

            while ((line = reader.readLine()) != null) {
                producer.send(new ProducerRecord<>(AppConfig.KAFKA_TOPIC, null, line));
                sentCount++;
            }
            producer.flush();

            long duration = System.currentTimeMillis() - startTime;
            System.out.printf("[METRIC] [KAFKA LOADER] Загрузка истории завершена: %d строк за %.2f сек%n",
                    sentCount, duration / 1000.0);

        } catch (IOException e) {
            System.err.println("[ERROR] [KAFKA LOADER] Сбой отправки данных: " + e.getMessage());
        }
    }
}