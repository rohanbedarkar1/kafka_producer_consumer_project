package com.github.rohanbedarkar1.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
       //create Producer properties
        Properties properties = new Properties();
        String bootstrapServers ="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //following two lines analyze and help kafka know the type of data being sent to kafka

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        //send the data //this is asynchronous
        producer.send(record);

        //to wait for the data to be sent
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
