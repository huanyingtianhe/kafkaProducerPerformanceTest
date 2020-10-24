package org.microsoft.com.kafkaProducerPerfTest;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class PorducerPerfTest {


    public static void main(String[] args) {
        Options options = new Options();

        Option nMessage = new Option("n", "num-message", true, "number of message");
        nMessage.setRequired(true);
        options.addOption(nMessage);

        Option sMessage = new Option("s", "size-message", true, "size of message");
        sMessage.setRequired(true);
        options.addOption(sMessage);


        Option isKafka = new Option("k", "kafka", true, "whether send to kafka, else eventserver");
        isKafka.setRequired(true);
        options.addOption(isKafka);

        Option isDebug = new Option("d", "debug", true, "show debug info");
        isDebug.setRequired(false);
        options.addOption(isDebug);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            return;
        }

        String nMessageValue = cmd.getOptionValue('n');
        String sMessageValue = cmd.getOptionValue('s');
        String isDebugValue = cmd.getOptionValue('d', "false");
        String isKafkaValue = cmd.getOptionValue('k', "true");

        long events = Long.parseLong(nMessageValue);
        long bit = Long.parseLong(sMessageValue);
        boolean isdebug = isDebugValue.equals("true");
        boolean isSendtoKafka = isKafkaValue.equals("true");
        Logger logger = org.apache.log4j.Logger.getLogger(PorducerPerfTest.class.getName());
        logger.info("message number:" + events);
        logger.info("message size:" + bit);

        Random rnd = new Random();
        //String brokers = "10.230.197.147:9092,10.230.197.146:9092,10.230.197.145:9092,10.230.197.144:9092,10.230.197.143:9092,10.230.197.142:9092,10.230.197.141:9092,10.230.197.140:9092,10.230.197.139:9092,10.230.197.138:9092,10.230.197.137:9092,10.230.197.136:9092,10.230.197.135:9092,10.230.197.134:9092,10.230.197.133:9092,10.230.197.131:9092,10.230.197.129:9092,10.230.197.128:9092,10.230.197.127:9092,10.230.197.126:9092,10.230.197.125:9092,10.230.197.124:9092,10.230.197.123:9092,10.230.197.122:9092,10.230.197.121:9092,10.230.197.120:9092,10.230.197.119:9092,10.230.197.118:9092,10.230.197.113:9092,10.230.197.112:9092";
        JsonUtil jsonUtil = new JsonUtil();
        String brokers = jsonUtil.getBrokers();
        Properties props = new Properties();
        props.setProperty("metadata.broker.list", brokers);
        props.setProperty("compression.codec", "gzip");
        props.setProperty("request.required.acks", "1");
        props.setProperty("partitioner.class", "org.microsoft.com.kafkaProducerPerfTest.CustomPartitioner");
        props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        //props.setProperty("send.buffer.bytes", (64 * 1024).toString)
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        MessagePoster mp = new MessagePoster();
        long startTime = System.currentTimeMillis();
        for (long nEvents = 0; nEvents < events; nEvents++) {

            String ip = "";
            for (int i = 0; i < bit; i++)
            {
                ip += rnd.nextInt(10);
            }
            String msg = ip;
            if(isdebug){
                logger.debug("send data: key " + ip + ", value: " + msg);
            }
            if(isSendtoKafka) {
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("test5", ip, msg);
                producer.send(data);
            }else{
                mp.postMessage("test5", ip);
            }
        }
        long endTime = System.currentTimeMillis();
        long bytes = events * bit / 8;
        logger.info("run time：" + (endTime - startTime) + "ms, speed: " + bytes * 1000 / (endTime - startTime) + " byte/s");
        producer.close();
    }
}
