package com.github.harish.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //Create The Properties
        Properties consumerProperties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-codebasefirsttopic-application-newconsumergroupId";
        String topic = "firsttopic";

        //Create Consumer Configs
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(consumerProperties);


        //Subscribe to the topics list
        consumer.subscribe(Collections.singleton(topic));
        //poll for the new data

        while(true)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record:records )
            {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() );
                logger.info("offset: " + record.offset());

            }
        }
    }
}
