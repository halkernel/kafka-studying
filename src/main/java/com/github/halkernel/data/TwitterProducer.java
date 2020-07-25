package com.github.halkernel.data;

import com.github.halkernel.config.TokenConfig;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String key;
    private String secret;
    private String accessToken;
    private String accessSecret;

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client client = createTwitterClient(msgQueue);
        client.connect();
        //TODO
        //create twitter client
        //create kafka producer
        //loop to send tweets to kafka

        while (!client.isDone()) {
            String message = null;
            try {
                message = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(message != null){
                logger.info(message);
            }
        }
        client.stop();
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        this.setValues();
        Authentication hosebirdAuth = new OAuth1(key, secret, accessToken, accessSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }


    private void setValues() {
        TokenConfig tokenConfig= new TokenConfig();

        Properties properties = null;
        try {
            properties = tokenConfig.getProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.key = properties.getProperty("apiKey");
        this.secret = properties.getProperty("apiSecret");
        this.accessToken = properties.getProperty("accessToken");
        this.accessSecret = properties.getProperty("accessSecret");

    }
}
