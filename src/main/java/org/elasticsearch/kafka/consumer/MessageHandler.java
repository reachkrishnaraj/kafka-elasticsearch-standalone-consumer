package org.elasticsearch.kafka.consumer;

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
	private BulkRequestBuilder bulkReqBuilder;
	private IndexHandler indexHandler;
		
	public MessageHandler(Client client,ConsumerConfig config) throws Exception{
		this.esClient = client;
		this.config = config;
		this.bulkReqBuilder = null;
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
		return bulkReqBuilder;
	}

	public void setBuildReqBuilder(BulkRequestBuilder bulkReqBuilder) {
		this.bulkReqBuilder = bulkReqBuilder;
	}
		
	public boolean postToElasticSearch() throws Exception {
		BulkResponse bulkResponse = null;
		BulkItemResponse bulkItemResp = null;
		//Nothing/NoMessages to post to ElasticSearch
		if(bulkReqBuilder.numberOfActions() <= 0){
			logger.warn("No messages to post to ElasticSearch - returning");
			return true;
		}		
		try{
			bulkResponse = bulkReqBuilder.execute().actionGet();
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
			while (bulkRespItr.hasNext()){
				bulkItemResp = bulkRespItr.next();
				if (bulkItemResp.isFailed()) {
					failedCount++;
					//Need to handle failure messages logging in a better way
					logger.error("Failed Message #{}: {}", failedCount, bulkItemResp.getFailure().getMessage());
				} else {
							//Do stats handling here
				}
					
			}								
			logger.info("# of failed to post messages to ElasticSearch: {} ", failedCount);
			// TODO log failed messages somewhere
			// it does not matter what is returned - as it is ignored now, to avoid 
			// re-processing the same failed messages over and over
			return false;				
		}
		logger.info("Bulk Post to ElasticSearch finished OK");
		bulkReqBuilder = null;
		return true;
	}

	public abstract byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception;
	
	public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator){
		bulkReqBuilder = esClient.prepareBulk();
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
				// TODO decide whether you want to fail the whole batch if transformation 
				// of one message fails, or if you just want to log this message into failedEvents.log
				// for later re-processing
				// for now - just log and continue
				logger.error("ERROR transforming message at offset={} - skipping it: {}", 
						messageAndOffset.offset(), new String(bytesMessage), e);
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

	
}
