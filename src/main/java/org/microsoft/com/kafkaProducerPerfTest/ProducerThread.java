package org.microsoft.com.kafkaProducerPerfTest;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ProducerThread implements Runnable {
    CmdConfig cmdConfig;
    Logger logger = org.apache.log4j.Logger.getLogger(ProducerThread.class.getName());
    CountDownLatch latch;
    public ProducerThread(CmdConfig conf, CountDownLatch la){
        cmdConfig = conf;
        latch = la;
    }
    public void run(){
        logger.info("message number:" + cmdConfig.events);
        logger.info("message size:" + cmdConfig.bit);

        Random rnd = new Random();
        //String brokers = "10.230.197.147:9092,10.230.197.146:9092,10.230.197.145:9092,10.230.197.144:9092,10.230.197.143:9092,10.230.197.142:9092,10.230.197.141:9092,10.230.197.140:9092,10.230.197.139:9092,10.230.197.138:9092,10.230.197.137:9092,10.230.197.136:9092,10.230.197.135:9092,10.230.197.134:9092,10.230.197.133:9092,10.230.197.131:9092,10.230.197.129:9092,10.230.197.128:9092,10.230.197.127:9092,10.230.197.126:9092,10.230.197.125:9092,10.230.197.124:9092,10.230.197.123:9092,10.230.197.122:9092,10.230.197.121:9092,10.230.197.120:9092,10.230.197.119:9092,10.230.197.118:9092,10.230.197.113:9092,10.230.197.112:9092";
        JsonUtil jsonUtil = new JsonUtil();
        String brokers = jsonUtil.getBrokers();
        ArrayList<String> topics = jsonUtil.getTopics();
        Properties props = new Properties();
        props.setProperty("metadata.broker.list", brokers);
        props.setProperty("compression.codec", jsonUtil.getProperty("compression.codec","gzip"));
        props.setProperty("request.required.acks", jsonUtil.getProperty("request.required.acks","1"));
        props.setProperty("partitioner.class", "org.microsoft.com.kafkaProducerPerfTest.CustomPartitioner");
        props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        props.setProperty("send.buffer.bytes", jsonUtil.getProperty("send.buffer.bytes", "102400"));
        if(!jsonUtil.isSync()) {
            props.setProperty("producer.type","async");
            props.setProperty("batch.num.messages", jsonUtil.getProperty("batch.num.messages", "200"));
            props.setProperty("queue.enqueue.timeout.ms", "-1");
        }
        props.setProperty("client.id", "ProducerPerformance");
        props.setProperty("request.timeout.ms", jsonUtil.getProperty("request.timeout.ms", "10000"));
        props.setProperty("message.send.max.retries", jsonUtil.getProperty("message.send.max.retries", "3"));
        props.setProperty("retry.backoff.ms", jsonUtil.getProperty("retry.backoff.ms", "100"));

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        MessagePoster mp = new MessagePoster();
        for (long nEvents = 0; nEvents < cmdConfig.events; nEvents++) {

            String ip = "";
            for (int i = 0; i < cmdConfig.bit; i++)
            {
                ip += rnd.nextInt(10);
            }
            String msg = ip;
            if(cmdConfig.isDebug){
                logger.debug("send data: key " + ip + ", value: " + msg);
            }
            for(String topic : topics) {
                if(cmdConfig.isSendKafka) {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
                    producer.send(data);
                }else{
                    mp.postMessage(topic, ip);
                }
            }

        }
        producer.close();
        latch.countDown();
    }
}
