package com.github.rohanbedarkar1.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("Duplicates")

public class ProducerDemoKeys {
    public static void main(String[] args) {
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
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world "+ Integer.toString(i));
            //send the data //this is asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadProducerDemoWithCallbackata, Exception e) {
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
            });
        }


        //to wait for the data to be sent
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
