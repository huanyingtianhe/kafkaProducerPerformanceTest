package org.microsoft.com.kafkaProducerPerfTest;

import org.apache.commons.cli.*;

public class CmdConfig {
    long events;
    long bit ;
    boolean isDebug;
    boolean isSendKafka;
    int threadNum;
    public void parseCmd(String[] args) throws ParseException {
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

        Option threadNumOpt = new Option("t", "threads", true, "thread number");
        threadNumOpt.setRequired(true);
        options.addOption(threadNumOpt);


        Option isDebugOpt = new Option("d", "debug", true, "show debug info");
        isDebugOpt.setRequired(false);
        options.addOption(isDebugOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java -jar kafkaProducerPerformanceTest-1.0-SNAPSHOT.jar [option]", options);
            throw e;
        }

        String nMessageValue = cmd.getOptionValue('n');
        String sMessageValue = cmd.getOptionValue('s');
        String isDebugValue = cmd.getOptionValue('d', "false");
        String isKafkaValue = cmd.getOptionValue('k', "true");
        String threadNumValue = cmd.getOptionValue('t', "1");

        events = Long.parseLong(nMessageValue);
        bit = Long.parseLong(sMessageValue);
        isDebug = isDebugValue.equals("true");
        isSendKafka = isKafkaValue.equals("true");
        threadNum = Integer.parseInt(threadNumValue);
        events = events / threadNum; // we send the same number message for every thread
    }
}
