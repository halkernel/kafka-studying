package com.github.halkernel.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {


        String bootstrapServer = "127.0.0.1:9092";
        String message = String.format("hello from %s", ProducerDemo.class.getSimpleName());
        System.out.println(message);

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord
                = new ProducerRecord<String, String>("first_topic", message);

        //send data
        producer.send(producerRecord);

        //flush and close producer
        producer.close();
    }

}
