package org.elasticsearch.kafka.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailedEventsLogger {

	private static final Logger logger = LoggerFactory.getLogger(FailedEventsLogger.class);

	public static void logFailedEvent(String errorMsg, String event){
		logger.error("General Error Processing Event: ERROR: {}, EVENT: {}", errorMsg, event);
	}

	public static void logFailedToPostToESEvent(String restResponse, String errorMsg){
		logger.error("Error posting event to ES: REST response: {}, ERROR: {}", restResponse, errorMsg);
	}

	public static void logFailedToTransformEvent(long offset, String errorMsg, String event){
		logger.error("Error transforming event: OFFSET: {}, ERROR: {}, EVENT: {}", 
				offset, errorMsg, event);
	}
	public static void logFailedEvent(long startOffset,long endOffset, int partition ,String errorMsg, String event){
		logger.error("Error transforming event: OFFSET: {} --> {} PARTITION: {},EVENT: {},ERROR: {} ",
				startOffset,endOffset, partition,event,errorMsg);
	}

}
