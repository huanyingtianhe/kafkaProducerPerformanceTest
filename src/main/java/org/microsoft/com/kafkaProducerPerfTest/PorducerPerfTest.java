package org.microsoft.com.kafkaProducerPerfTest;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class PorducerPerfTest {

    public static void main(String[] args) {
        Logger logger = org.apache.log4j.Logger.getLogger(PorducerPerfTest.class.getName());
        try {
            CmdConfig cmdline = new CmdConfig();
            cmdline.parseCmd(args);
            long startTime = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(cmdline.threadNum);
            CountDownLatch latch = new CountDownLatch(cmdline.threadNum);
            ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
            for (int i = 0; i < cmdline.threadNum; i++) {
                executor.submit(new ProducerThread(cmdline, latch));
            }
            try {
                logger.info("waitting for send request to kafka");
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            JsonUtil jsonUtil = new JsonUtil();
            ArrayList<String> topics = jsonUtil.getTopics();
            long bytes = topics.size() * cmdline.events * cmdline.bit / 8;
            logger.info("run time：" + (endTime - startTime) + "ms, speed: " + bytes * 1000 / (endTime - startTime) + " byte/s");
        }catch(ParseException e){
            return;
        }
    }
}
