package org.erturk.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    // 1. Create Logger
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        // 1.1. Adding log to show that the Producer class has been called
        log.info("I am a Kafka Producer!");

        // 2. Create Producer Properties
        Properties properties = getProperties();

        // 3. Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 4. Create Producer Record
         ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // 5. Send data
        kafkaProducer.send(producerRecord);

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
        // properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2.2 Connect to Conduktor Playground
        // properties.setProperty("bootstrap.servers", "cdk-gateway:6969");
        // properties.setProperty("security.protocol", "SASL_SSL");
        // properties.setProperty("sasl.mechanism", "PLAIN");
        // properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='x@mail.com' password='xxx'");

        return properties;
    }
}
