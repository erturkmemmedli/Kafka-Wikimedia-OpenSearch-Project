package org.erturk.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    // 1. Create Logger
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        // 1.1. Adding log to show that the Producer class has been called
        log.info("I am a Kafka Producer!");

        // 2. Create Consumer Properties
        Properties properties = getProperties();

        // 3. Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topic = "demo_java";

        // 6. Get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        // 7. Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // 8. Add try block
        try {
            // 4. Subscribe to topic
            consumer.subscribe(List.of(topic));

            // 5. Poll for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer and commit offsets
            log.info("The consumer is now gracefully shutdown");
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
