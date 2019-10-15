package com.github.rohanbedarkar1.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("Duplicates")
public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new com.github.rohanbedarkar1.kafka.tutorial1.ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){

    }
    public void run(){
        Logger logger = LoggerFactory.getLogger((ConsumerDemoWithThreads.class.getName()));

        String bootstrapServers ="127.0.0.1:9092";
        String groupId ="my-sixth-application";
        String topic="first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        logger.info("Creating the consumer");
        Runnable myConsumerRunnable = new ConsumerRunnable( bootstrapServers, groupId, topic, latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted-",e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger((ConsumerRunnable.class.getName()));
        public ConsumerRunnable( String bootstrapServers, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create a consumer
            consumer = new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topic(s)

            consumer.subscribe(Arrays.asList("first_topic"));

        }

        @Override
        public void run() {
            //poll for new data
            //consumer does not get the data until it asks for data
            try {
                while (true) {
                    //consumer.poll(100); // new in Kafka 2.0.0 onwards , deprecated
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key:" + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Recieved shutdown signal");
                //e.printStackTrace();
            }finally {
                consumer.close();
                //tell our main code we're done with the consumer
                latch.countDown();
            }
        }
        public void shutdown(){
            //the wakeup method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException

            consumer.wakeup();

        }
    }
}
