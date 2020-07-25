package com.github.halkernel.data;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TwitterProducer {

    public void run(){
        //TODO
        //create twitter client
        //create kafka producer
        //loop to send tweets to kafka
    }

    public void createTwitterClient(){
        //setting up blocking queues
        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<>(100000);
    }
}
