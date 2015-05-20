package org.elasticsearch.kafka.consumer;

import java.util.Iterator;

import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;


public abstract class MessageHandler {

	private Client esClient;
	private ConsumerConfig config;
	private BulkRequestBuilder bulkReqBuilder;
	Logger logger = ConsumerLogger.getLogger(this.getClass());
		
	public MessageHandler(){
	}
	
	public Client getEsClient() {
		return esClient;
	}

	public void setEsClient(Client esClient) {
		this.esClient = esClient;
	}

	public ConsumerConfig getConfig() {
		return config;
	}

	public void setConfig(ConsumerConfig config) {
		this.config = config;
	}

	public BulkRequestBuilder getBuildReqBuilder() {
		return bulkReqBuilder;
	}

	public void setBuildReqBuilder(BulkRequestBuilder bulkReqBuilder) {
		this.bulkReqBuilder = bulkReqBuilder;
	}
	
	public void initMessageHandler(Client client,ConsumerConfig config){
		this.esClient = client;
		this.config = config;
		this.bulkReqBuilder = null;
		logger.info("Initialized Message Handler");
	}
	
	public boolean postToElasticSearch() throws Exception {
		BulkResponse bulkResponse = null;
		BulkItemResponse bulkItemResp = null;
		//Nothing/NoMessages to post to ElasticSearch
		if(this.bulkReqBuilder.numberOfActions() <= 0){
			logger.warn("BulkReqBuilder doesnt have any messages to post to ElasticSearch.Will simply return to main ConsumerJob");
			return true;
		}		
		try{
			bulkResponse = this.bulkReqBuilder.execute().actionGet();
		}
		catch(ElasticsearchException e){
			logger.fatal("Failed to post the messages to ElasticSearch. Throwing the error :: " + e.getMessage(), e);
			throw e;
		}
		logger.info("Time took to post the bulk messages to post to ElasticSearch [ms] :: " + bulkResponse.getTookInMillis());
		if(bulkResponse.hasFailures()){
			logger.error("Bulk Message Post to ElasticSearch has errors. Failure message is :: " + 
					bulkResponse.buildFailureMessage());
				int failedCount = 1;
				Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
				while (bulkRespItr.hasNext()){
					bulkItemResp = bulkRespItr.next();
					if (bulkItemResp.isFailed()) {
						//Need to handle failure messages logging in a better way
						logger.error("Failed Message # " + failedCount + " is::" + bulkItemResp.getFailure().getMessage());
						failedCount++;
					} else {
							//Do stats handling here
					}
					
				}
								
				logger.info("# of failed to post messages to ElasticSearch is :: " + failedCount);
				// TODO log failed messages somewhere - but do not return FALSE  - to stop processing
				// the same messages over and over
				return true;
				/*
				int msgFailurePercentage = (Integer)((failedCount/this.getSizeOfOffsetMsgMap()) * 100); 
				logger.info("% of failed message post to ElasticSearch is:: " + msgFailurePercentage);
				logger.info("ElasticSearch msg failure tolerance % is::" + this.config.esMsgFailureTolerancePercent);
				if(msgFailurePercentage > this.config.esMsgFailureTolerancePercent){
					logger.error("% of failed messages is GREATER than set tolerance.Hence would return to consumer job with FALSE");
					this.bulkReqBuilder = null;
					return false;
				}
				else{
					logger.info("% of failed messages is LESSER than set tolerance.Hence would return to consumer job with TRUE");
					this.bulkReqBuilder = null;
					return true;
				}
					/*  */
				
			}
		logger.info("Bulk Post to ElasticSearch was success with no single error. Returning to consumer job with true.");
		this.bulkReqBuilder = null;
		return true;
	}

	public abstract byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception;
	
	public abstract long prepareForPostToElasticSearch(
		Iterator<MessageAndOffset> messageAndOffsetIterator) throws Exception;

	
}
