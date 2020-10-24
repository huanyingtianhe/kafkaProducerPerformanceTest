package org.microsoft.com.kafkaProducerPerfTest;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsontUtilTest {
    @Test
    public void testGetProperty(){
        JsonUtil jsons = new JsonUtil();
        assertEquals(jsons.getProperty("topics", ""), "test5");
    }

    @Test
    public void testGetPartitions(){
        JsonUtil jsons = new JsonUtil();
        ArrayList<Integer> partitions = jsons.getPartitions();
        assertEquals(partitions.size(), 12);
        System.out.println(partitions);
    }
}
