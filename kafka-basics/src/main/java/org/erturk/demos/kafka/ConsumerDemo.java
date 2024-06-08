package org.erturk.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    // 1. Create Logger
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        // 1.1. Adding log to show that the Producer class has been called
        log.info("I am a Kafka Producer!");

        // 2. Create Consumer Properties
        Properties properties = getProperties();

        // 3. Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topic = "demo_java";

        // 4. Subscribe to topic
        consumer.subscribe(List.of(topic));

        // 5. Poll for data
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        String groupId = "my-java-application";

        // 2.1. Connect to broker in Docker
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 2.2. Create Consumer Configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // none, earliest, latest

        return properties;
    }
}
