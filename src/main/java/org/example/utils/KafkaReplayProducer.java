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

public class KafkaReplayProducer {
    public static void main(String[] args) {
        String streamFilePath = AppConfig.getStreamPath();
        int targetEventsPerSecond = 1000;
        long delayBetweenMessagesNs = 1_000_000_000L / targetEventsPerSecond;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");

        System.out.println("[SYSTEM] [KAFKA PRODUCER] Запуск симуляции потока из " + streamFilePath + " (" + targetEventsPerSecond + " сообщ/сек)...");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(Paths.get(streamFilePath))) {

            reader.readLine();
            String line;
            long sentCount = 0;

            while ((line = reader.readLine()) != null) {
                producer.send(new ProducerRecord<>(AppConfig.KAFKA_TOPIC, null, line));
                sentCount++;

                if (delayBetweenMessagesNs > 0) {
                    long targetTimeNs = System.nanoTime() + delayBetweenMessagesNs;
                    while (System.nanoTime() < targetTimeNs) {
                    }
                }
            }
            System.out.println("[SYSTEM] [KAFKA PRODUCER] Симуляция потока завершена. Всего строк: " + sentCount);

        } catch (IOException e) {
            System.err.println("[ERROR] [KAFKA PRODUCER] Сбой симуляции потока: " + e.getMessage());
        }
    }
}