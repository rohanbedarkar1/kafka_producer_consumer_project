package com.github.rohanbedarkar1.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger((ConsumerDemo.class.getName()));

        String bootstrapServers ="127.0.0.1:9092";
        String groupId ="my-fourth-application";
        String topic="first_topic";

        //when the producer takes a string , serializes it to bytes and sends it to kafka, when kafka sends these bytes
        // to right back to our consumers
        // our consumers have to these bytes and create a string from it
        //this process is called deserialization
        // thats why we provide a stringDeserializer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest/latest/none");
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //read from only the new messages
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")// will throw an error if no offsets are being saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topics (s)
        //consumer.subscribe(Collections.singleton("first_topic"));
        //consumer.subscribe(Collections.singleton(topic));
        //consumer.subscribe(Arrays.asList("first_topic","second_topic","third_topic"));
        consumer.subscribe(Arrays.asList("first_topic"));

        //poll for new data
        //consumer does not get the data until it asks for data
        while(true){
            //consumer.poll(100); // new in Kafka 2.0.0 onwards , deprecated
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){
                logger.info("Key:" +record.key()+", Value: " +record.value());
                logger.info("Partition: "+record.partition() + ", Offset: "+record.offset());
            }
        }

    }
}
