package com.github.rohanbedarkar1.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("Duplicates")

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
       //create Producer properties
        Properties properties = new Properties();
        String bootstrapServers ="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //following two lines analyze and help kafka know the type of data being sent to kafka

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10;i++){
            //create a producer record
            String topic ="first_topic";
            String value ="hello world" + Integer.toString(i);
            String key= "id_"+ Integer.toString(i);
            //
            // final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world "+ Integer.toString(i));
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            //ProducerRecord<String, String> record = new ProducerRecord<~>(topic, key, value);

            logger.info("Key:" +key); //log the key
            //id_0 is going to partition 1
            //id_1 partition 0
            //id_2 partition 2
            //id_3 partition 0
            //id_4 partition 2
            //id_5 partition 2
            //id_6 partition 0
            //id_7 partition 2
            //id_8 partition 1
            //id_9 partition 2
            //same key goes to same partition everytime
            //id is similar to truck id
            //create a topic with 5 partition then every time diff partition mapping


            //send the data //this is asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record  is sucessfully sent or an exception is thrown
                    if(e ==null){
                        //the record was successfully sent
                        //before this see the logger
                        logger.info("Recieved new metadata. \n" +
                                "Topic: " +recordMetadata.topic() +"\n" +
                                "Partition: " +recordMetadata.partition() +"\n" +
                                "Offset: " + recordMetadata.offset() +"\n"+
                                "Timestamp " + recordMetadata.timestamp());

                    }else {
                        //e.printStackTrace();
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send() to make it synchronous - dont't do this in production
        }


        //to wait for the data to be sent
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
