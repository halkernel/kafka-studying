package thread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable{

    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch,
                          Properties properties,
                          String topic){
        this.latch = latch;
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try{
            while (true){ //just for understanding
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record: consumerRecords) {
                    logger.info("Key: " + record.key() + " " + "Value: " + record.value());
                    logger.info("Partition: " + record.partition());
                    logger.info("Offset: " + record.offset());
                }
            }
        }catch (WakeupException ex){
            logger.info("Received shutdown signal!");
        }finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void dropConsumer(){
        //this method interrupt the consumer.poll()
        //it will throw the WakeupException
        consumer.wakeup();
    }
}
