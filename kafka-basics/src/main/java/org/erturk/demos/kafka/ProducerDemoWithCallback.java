package org.erturk.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    // 1. Create Logger
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        // 1.1. Adding log to show that the Producer class has been called
        log.info("I am a Kafka Producer!");

        // 2. Create Producer Properties
        Properties properties = getProperties();

        // 3. Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int j=0; j < 10; j++) {
            for (int i = 0; i < 10; i++) {
                // 4. Create Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "meselcun " + i);

                // 5. Send data
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // Executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // The record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // 6. Tell the Producer to send all data and block until done -- synchronous
        kafkaProducer.flush();

        // 7. Close the Producer
        kafkaProducer.close();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        // 2.1. Connect to broker in Docker
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "500");
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        return properties;
    }
}
