package com.github.halkernel.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        final String bootstrapServers = "127.0.0.1:9092";
        final String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"earliest" "latest" or "none"

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data, fetch an specific message

        //assign
        TopicPartition partition = new TopicPartition(topic, 0);
        Long offset = 15L;
        consumer.assign(Arrays.asList(partition));

        //seek
        consumer.seek(partition, offset);

        int messagesToReadBeforeExit = 5;
        int messagesRead = 0;
        boolean keepReading = true;


        while (keepReading){ //just for understanding
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record: consumerRecords) {
                messagesRead+=1;
                logger.info("Key: " + record.key() + " " + "Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("Offset: " + record.offset());
                if(messagesRead >= messagesToReadBeforeExit){
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting application");


    }
}
