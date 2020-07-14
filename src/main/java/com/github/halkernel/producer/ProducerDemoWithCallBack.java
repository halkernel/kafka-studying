package com.github.halkernel.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {


    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        String bootstrapServer = "127.0.0.1:9092";
        String message = String.format("hello from %s", ProducerDemoWithCallBack.class.getSimpleName());
        System.out.println(message);

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for (int i = 0; i < 10; i++) {

            //create producer record
            ProducerRecord<String, String> producerRecord
                    = new ProducerRecord<String, String>("first_topic", message + " " + i);

            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is successfully sent or an exception is thrown
                    if(e == null){
                        //the record was sent
                        logger.info(
                                String.format("\n Received new metadata -- \n Topic: %s \n Partition: %s \n Offset: %s \n Timestamp: %s \n",
                                        recordMetadata.topic(),
                                        recordMetadata.partition(),
                                        recordMetadata.offset(),
                                        recordMetadata.timestamp())
                        );
                    }else{
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        //send data - asynchronously


        //flush and close producer
        producer.close();
    }

}
