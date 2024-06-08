package org.erturk.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    // 1. Create Logger
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        // 1.1. Adding log to show that the Producer class has been called
        log.info("I am a Kafka Producer!");

        // 2. Create Producer Properties
        Properties properties = getProperties();

        // 3. Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "demo_java";

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = "new data " + i;

            // 4. Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // 5. Send data
            kafkaProducer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    log.info("Key: " + key + " | Partition: " + metadata.partition());
                } else {
                    log.error("Error while producing", e);
                }
            });

            Thread.sleep(500);
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

        return properties;
    }
}
