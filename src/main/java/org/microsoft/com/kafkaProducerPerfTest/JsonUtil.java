package org.microsoft.com.kafkaProducerPerfTest;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class JsonUtil {
    Logger logger = org.apache.log4j.Logger.getLogger(JsonUtil.class.getName());
    public String getBrokers(){
        String brokers="";
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("conf.json"));
            JSONObject jsonObject = (JSONObject) obj;
            brokers = (String)jsonObject.get("brokers");
        }catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logger.info(brokers);
        return brokers;
    }

    public ArrayList<Integer> getPartitions(){
        ArrayList<Integer> partitions = new ArrayList<>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("conf.json"));
            JSONObject jsonObject =  (JSONObject) obj;
            JSONArray partitionsArray = (JSONArray)jsonObject.get("partitions");
            for(Object o : partitionsArray){
                String part = (String)o;
                if(part.contains("-")){
                    String[] ins = part.split("-");
                    if(ins.length != 2) {
                        logger.error("partition conf error, you should specify exactly 2 int when there are '-', please modify it in conf.json");
                        throw new IOException();
                    }
                    String start = ins[0].trim();
                    String end = ins[1].trim();
                    for(int s = Integer.parseInt(start); s <= Integer.parseInt(end); s++){
                        partitions.add(s);
                    }
                }else{
                    partitions.add(Integer.parseInt(part.trim()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logger.info(partitions);
        return partitions;
    }

    public ArrayList<String> getTopics(){
        ArrayList<String> topicsR = new ArrayList<>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("conf.json"));
            JSONObject jsonObject = (JSONObject) obj;
            String topics = (String)jsonObject.get("topics");
            String[] topicArray = topics.split(",");
            for(String topic : topicArray){
                topicsR.add(topic.trim());
            }
        }catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logger.info(topicsR);
        return topicsR;
    }
}
