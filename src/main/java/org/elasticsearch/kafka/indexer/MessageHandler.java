package org.elasticsearch.kafka.indexer;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
	private Client esClient;
	private ConsumerConfig config;
	private BulkRequestBuilder bulkRequestBuilder;
	private IndexHandler indexHandler;
		
	public MessageHandler(Client client,ConsumerConfig config) throws Exception{
		this.esClient = client;
		this.config = config;
		this.bulkRequestBuilder = null;
		// instantiate specified in the config IndexHandler class
		try {
			indexHandler = (IndexHandler) Class
					.forName(config.indexHandlerClass)
					.getConstructor(ConsumerConfig.class)
					.newInstance(config);
			logger.info("Created IndexHandler: ", config.indexHandlerClass);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException
				| ClassNotFoundException e) {
			logger.error("Exception creating IndexHandler: " + e.getMessage(), e);
			throw e;
		}
		logger.info("Created Message Handler");
	}
	
	public Client getEsClient() {
		return esClient;
	}

	public ConsumerConfig getConfig() {
		return config;
	}

	public BulkRequestBuilder getBuildReqBuilder() {
		return bulkRequestBuilder;
	}

	public void setBuildReqBuilder(BulkRequestBuilder bulkReqBuilder) {
		this.bulkRequestBuilder = bulkReqBuilder;
	}

	
	public boolean postToElasticSearch() throws Exception {
		BulkResponse bulkResponse = null;
		BulkItemResponse bulkItemResp = null;
		//Nothing/NoMessages to post to ElasticSearch
		if(bulkRequestBuilder.numberOfActions() <= 0){
			logger.warn("No messages to post to ElasticSearch - returning");
			return true;
		}		
		try{
			bulkResponse = bulkRequestBuilder.execute().actionGet();
		}
		catch(ElasticsearchException e){
			logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
			throw e;
		}
		logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
		if(bulkResponse.hasFailures()){
			logger.error("Bulk Message Post to ElasticSearch has errors: {}", 
					bulkResponse.buildFailureMessage());
			int failedCount = 0;
			Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
			//TODO research if there is a way to get all failed messages without iterating over
			// ALL messages in this bulk post request
			while (bulkRespItr.hasNext()){
				bulkItemResp = bulkRespItr.next();
				if (bulkItemResp.isFailed()) {
					failedCount++;
					String errorMessage = bulkItemResp.getFailure().getMessage();
					String restResponse = bulkItemResp.getFailure().getStatus().name();
					logger.error("Failed Message #{}, REST response:{}; errorMessage:{}", 
							failedCount, restResponse, errorMessage);
					// TODO: there does not seem to be a way to get the actual failed event
					// until it is possible - do not log anything into the failed events log file
					//FailedEventsLogger.logFailedToPostToESEvent(restResponse, errorMessage);
				} 					
			}								
			logger.info("# of failed to post messages to ElasticSearch: {} ", failedCount);
			return false;				
		}
		logger.info("Bulk Post to ElasticSearch finished OK");
		bulkRequestBuilder = null;
		return true;
	}

	public abstract byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception;
	
	public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator){
		bulkRequestBuilder = esClient.prepareBulk();
		int numProcessedMessages = 0;
		int numMessagesInBatch = 0;
		long offsetOfNextBatch = 0;
		while(messageAndOffsetIterator.hasNext()) {
			numMessagesInBatch++;
			MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
			offsetOfNextBatch = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			ByteBuffer payload = message.payload();
			byte[] bytesMessage = new byte[payload.limit()];
			payload.get(bytesMessage);
			byte[] transformedMessage;
			try {
				transformedMessage = this.transformMessage(bytesMessage, messageAndOffset.offset());
			} catch (Exception e) {
				String msgStr = new String(bytesMessage);
				logger.error("ERROR transforming message at offset={} - skipping it: {}", 
						messageAndOffset.offset(), msgStr, e);
				FailedEventsLogger.logFailedToTransformEvent(
						messageAndOffset.offset(), e.getMessage(), msgStr);
				continue;
			}
			this.getBuildReqBuilder().add(
				esClient.prepareIndex(
					indexHandler.getIndexName(null), indexHandler.getIndexType(null))
					.setSource(transformedMessage)
			);
			numProcessedMessages++;
		}
		logger.info("Total # of messages in this batch: {}; " + 
			"# of successfully transformed and added to Index messages: {}; offsetOfNextBatch: {}", 
			numMessagesInBatch, numProcessedMessages, offsetOfNextBatch);
		return offsetOfNextBatch;
	}

	public IndexHandler getIndexHandler() {
		return indexHandler;
	}

	
}
