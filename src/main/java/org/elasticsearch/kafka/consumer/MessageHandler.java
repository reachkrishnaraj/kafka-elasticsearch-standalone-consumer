package org.elasticsearch.kafka.consumer;

import java.util.Iterator;

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
		
	public MessageHandler(Client client,ConsumerConfig config){
		this.esClient = client;
		this.config = config;
		this.bulkReqBuilder = null;
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
		if(this.bulkReqBuilder.numberOfActions() <= 0){
			logger.warn("No messages to post to ElasticSearch - returning");
			return true;
		}		
		try{
			bulkResponse = this.bulkReqBuilder.execute().actionGet();
		}
		catch(ElasticsearchException e){
			logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
			throw e;
		}
		logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
		if(bulkResponse.hasFailures()){
			logger.error("Bulk Message Post to ElasticSearch has errors: {}", 
					bulkResponse.buildFailureMessage());
			int failedCount = 1;
			Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
			while (bulkRespItr.hasNext()){
				bulkItemResp = bulkRespItr.next();
				if (bulkItemResp.isFailed()) {
					//Need to handle failure messages logging in a better way
					logger.error("Failed Message #{}: {}", failedCount, bulkItemResp.getFailure().getMessage());
					failedCount++;
				} else {
							//Do stats handling here
				}
					
			}								
			logger.info("# of failed to post messages to ElasticSearch: {} ", failedCount);
			// TODO log failed messages somewhere - but do not return FALSE  - to stop processing
			// the same messages over and over
			return false;				
		}
		logger.info("Bulk Post to ElasticSearch finished OK");
		this.bulkReqBuilder = null;
		return true;
	}

	public abstract byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception;
	
	public abstract long prepareForPostToElasticSearch(
		Iterator<MessageAndOffset> messageAndOffsetIterator) throws Exception;

	
}
