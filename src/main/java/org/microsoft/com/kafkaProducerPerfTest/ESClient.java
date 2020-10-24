package org.microsoft.com.kafkaProducerPerfTest;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;


public class ESClient implements AutoCloseable{

    private static final Logger LOGGER = LoggerFactory.getLogger(ESClient.class);

    private final CloseableHttpClient httpClient;
    private final String esUrl;


    private int numberOfErrors;
    private int numberOfExceptions;

    public ESClient(String esUrl) {
        this.esUrl = esUrl;
        this.httpClient = HttpClientBuilder.createHttpClient();
        this.numberOfErrors = 0;
        this.numberOfExceptions = 0;
    }

    public String postToTopic(String topic, String msg ) {
        return postToTopic( topic, msg.getBytes(StandardCharsets.UTF_8));
    }


    public String postToTopic( String topic, byte[] msg ) {
        return doPost( "?type="+topic ,
                msg );
    }

    public String postToTopicDirect(String topic, String message, int partition) {
        return doPost( "?type="+topic +"&partitiontype=direct&partitionkey="+partition ,
                message.getBytes(StandardCharsets.UTF_8) );
    }

    public String getESTopicPartitionMetaData(String topic, int i) {
        return doGetString("/meta/v1/" + topic + "/" + i);
    }

    public byte[] consumeBondFrom(String topic, int partition, long offset) {
        return doGetBytes("/consume/v1/" + topic + "/" + partition + "/" + offset + "?format=bond");
    }

    public String getOffsetByTimestamp(String topic, int partition, long timestamp) {
        return doGetString("/offset/v1/"+topic+"/"+partition+"/"+timestamp);
    }

    public String postToTopicBatched(String topic , byte[][][] kvPairs ) {
        return doPost( "?type="+topic +"&sendtype=multi",
                encodeKvPairsAsBytes(kvPairs));
    }

    protected final String doPost(String relativeUrl, byte[] bytes) {
        try {
            HttpPost httpPost = new HttpPost( this.esUrl + relativeUrl);
            httpPost.setEntity(new ByteArrayEntity(bytes));
            LOGGER.debug("Executing request {}", httpPost.getRequestLine());
            try (CloseableHttpResponse response = this.httpClient.execute(httpPost)) {
                StatusLine status = response.getStatusLine();
                LOGGER.debug("{} {}", status, status.getReasonPhrase());
                if (status.getStatusCode() != 200) {
                    numberOfErrors++;
                    LOGGER.error("Unexpected response status: " + status);
                }

                HttpEntity respContent= response.getEntity();
                return IOUtils.toString(respContent.getContent(),StandardCharsets.UTF_8);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception when sending: " + ex);
            numberOfExceptions++;
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else throw new RuntimeException(ex);
        }
    }

    private String doGetString(String relativeUrl) {
        return new String(doGetBytes(relativeUrl), StandardCharsets.UTF_8);
    }

    private byte[] doGetBytes(String relativeUrl) {
        HttpGet httpGet = new HttpGet( this.esUrl+ relativeUrl);
        LOGGER.debug("Executing request {}", httpGet.getRequestLine());
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            StatusLine status = response.getStatusLine();
            LOGGER.debug("{} {}", status, status.getReasonPhrase());
            if (status.getStatusCode() != 200) {
                throw new RuntimeException("Unexpected response status: " + status + ", error msg: " + status.getReasonPhrase());
            }
            HttpEntity respContent = response.getEntity();
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            IOUtils.copy(respContent.getContent(),result);
            return result.toByteArray();
        } catch (Exception ex) {
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            }
            else throw new RuntimeException(ex);
        }
    }

    private byte[] encodeKvPairsAsBytes( byte[][][] kvPairs ) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < kvPairs.length; i++) {
                bos.write(encodeKvPairAsBytes(kvPairs[i][0], kvPairs[i][1]));
            }
            return bos.toByteArray();
        }
        catch(IOException ex) { throw new RuntimeException(ex);}
    }

    private byte[] encodeKvPairAsBytes(byte[] key, byte[] content) {
        if (key == null) {
            key= EMPTY_BYTES;
        }

        return ByteBuffer.allocate(4 + key.length + 4 + content.length)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(key.length)
                .put(key)
                .putInt(content.length)
                .put(content)
                .array();
    }

    private static final byte[] EMPTY_BYTES = new byte[] {};

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

}
