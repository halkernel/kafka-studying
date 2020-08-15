package consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class ElasticSearch {

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearch.class.getName());

        RestHighLevelClient client = RestHighLevelClientCreator.createClient();

        KafkaConsumer<String, String> consumer = RestHighLevelClientCreator.createConsumer("twitter_tweets");
        while (true) { //just for understanding, not recommendable

            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            Integer recordCount = consumerRecords.count();
            logger.info("received: " + recordCount);

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : consumerRecords) {

                //there are 2 strategies for creating ids
                // 1 kafka generic id:
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // 2 twitter feed specific id
                try{
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // this is to make our consumer idempotent
                    ).source(record.value().getBytes(StandardCharsets.US_ASCII), XContentType.JSON);

                    bulkRequest.add(indexRequest); //add to our bulkrequest
                }catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }

            }
            if(recordCount >= 0){
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("commiting the offsets");
                consumer.commitSync();
                logger.info("offsets have been commited");

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    private static String extractIdFromTweet(String tweet){
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
