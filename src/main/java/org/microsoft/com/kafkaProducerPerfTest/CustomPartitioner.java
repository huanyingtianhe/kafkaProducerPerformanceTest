package org.microsoft.com.kafkaProducerPerfTest;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.commons.cli.MissingOptionException;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomPartitioner implements Partitioner {
    Random rnd = new Random(System.currentTimeMillis());
    ArrayList<Integer> partitions = new ArrayList<>();

    public CustomPartitioner(VerifiableProperties props)
    {
        JsonUtil jsonUtil = new JsonUtil();
        partitions = jsonUtil.getPartitions();
    }

    public int partition(Object key, int numPartitions) {

        return partitions.get(rnd.nextInt(partitions.size()));
    }
}
