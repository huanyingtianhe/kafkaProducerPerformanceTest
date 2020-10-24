package org.microsoft.com.kafkaProducerPerfTest;

import org.apache.log4j.Logger;

public class MessagePoster {

	private final ESClient esClient;
    Logger LOGGER = org.apache.log4j.Logger.getLogger(MessagePoster.class.getName());

    public MessagePoster() {
	    String url = "localhost";
		this.esClient = new ESClient("https://localhost/EventServer");
	}

	public String postMessage(String topic, String msg) {
		try {
			return this.esClient.postToTopic(topic, msg);
		}catch(Exception e) {
			String errorMsg = "Error in posting data to topic " + topic;
			LOGGER.error(errorMsg);
			throw new RuntimeException(errorMsg, e);
		}
	}

	public String postMessage(String topic, byte[] msg) {
		try {
			return this.esClient.postToTopic(topic, msg);
		}catch(Exception e) {
			String errorMsg = "Error in posting data to topic " + topic;
			LOGGER.error(errorMsg);
			throw new RuntimeException(errorMsg, e);
		}
	}

}
