package com.github.harish.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer

        //id_0 goes to partition 1
        //id_1 goes to partition 0
        //id_2 goes to partition 2
        //id_3 goes to partition 0
        //id_4 goes to partition 2
        //id_5 goes to partition 2
        //id_6 goes to partition 0
        //id_7 goes to partition 2
        //id_8 goes to partition 1
        //id_9 goes to partition 2

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            {
                String topic = "firsttopic";
                String value = "Hello Chitti with MetaData :  " + Integer.toString(i);
                String key = "id_" +  Integer.toString(i);


                //Create Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key,value);

                logger.info("Key: "+key);

                //send the data  -asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes everytime, when record is sent successfully or an Exception is thrown
                        if (e == null) {
                            //record is successfully sent
                            logger.info("Recieved new Metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "TimeStamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error While Producing", e);
                        }

                    }
                }).get();  //block the .send() to make it synchronous ---Not for production
            }
            //flush the data
            producer.flush();

            //flush and close the data
           // producer.close();

        }
    }

}