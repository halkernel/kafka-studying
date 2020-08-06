package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thread.ConsumerThread;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);
        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "testing-sixth-application";
        final String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"earliest" "latest" or "none"

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //latch for doung with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //consumer runnable is created
        Runnable consumerThread = new ConsumerThread(latch, properties, topic);

        //starts the thread
        Thread thread = new Thread(consumerThread);
        thread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown Hook is Being Called");
            ((ConsumerThread) consumerThread).dropConsumer();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                logger.info("Application has exited");
            }
        }));

        //output should be:
        /*
        *
        * [Thread-2] INFO com.github.halkernel.consumer.ConsumerDemoThreads - Shutdown Hook is Being Called
        * [Thread-1] INFO com.github.halkernel.thread.ConsumerThread - Received shutdown signal!
        * [main] INFO com.github.halkernel.consumer.ConsumerDemoThreads - Application is down
        * [Thread-2] INFO com.github.halkernel.consumer.ConsumerDemoThreads - Application has exited
        * */

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
        }finally {
            logger.info("Application is closing");
        }

    }
}
