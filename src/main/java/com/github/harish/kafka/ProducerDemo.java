package com.github.harish.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        //Create Producer Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("firsttopic","Hello Chitti");

        //send the data  -asynchronous
        producer.send(producerRecord);

        //flush the data
        producer.flush();

        //flush and close the data
        producer.close();

    }
}
