package org.elasticsearch.kafka.consumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import kafka.message.Message;


public abstract class MessageHandler {

	private Client esClient;
	private LinkedHashMap<Long, Message> offsetMsgMap;
	private ConsumerConfig config;
	private BulkRequestBuilder bulkReqBuilder;
	private ArrayList<Object> esPostObject= new ArrayList<Object>();
	Logger logger = ConsumerLogger.getLogger(this.getClass());
		
	public MessageHandler(){
	}
	
	public Client getEsClient() {
		return esClient;
	}

	public void setEsClient(Client esClient) {
		this.esClient = esClient;
	}

	/*public void initMessageHandler(){
		this.esPostObject= new ArrayList<Object>();
		this.offsetMsgMap = new LinkedHashMap<Long, Message>();
		this.bulkReqBuilder = null;
		logger.info("Initialized Message Handler");
		
	}*/
	
	public LinkedHashMap<Long, Message> getOffsetMsgMap() {
		return offsetMsgMap;
	}

	public void setOffsetMsgMap(LinkedHashMap<Long, Message> offsetMsgMap) {
		this.offsetMsgMap = offsetMsgMap;
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
	
	public ArrayList<Object> getEsPostObject() {
		return esPostObject;
	}

	public void setEsPostObject(ArrayList<Object> esPostObject) {
		this.esPostObject = esPostObject;
	}

	public void initMessageHandler(Client client,ConsumerConfig config){
		this.esClient = client;
		this.config = config;
		this.esPostObject= new ArrayList<Object>();
		this.offsetMsgMap = new LinkedHashMap<Long, Message>();
		this.bulkReqBuilder = null;
		logger.info("Initialized Message Handler");
	}
	
	public boolean postToElasticSearch() throws Exception {
		BulkResponse bulkResponse = null;
		//Nothing/NoMessages to post to ElasticSearch
		if(this.bulkReqBuilder.numberOfActions() <= 0){
			logger.warn("BulkReqBuilder doesnt have any messages to post to ElasticSearch.Will simply return to main ConsumerJob");
			return true;
		}		
		try{
			bulkResponse = this.bulkReqBuilder.execute().actionGet();
		}
		catch(Exception e){
			logger.fatal("Failed to post the messages to ElasticSearch. Throwing the error. Error Message is::" + e.getMessage());
			
		}
		logger.info("Time took to post the bulk messages to post to ElasticSearch is::" + bulkResponse.getTookInMillis() + "milli seconds");
		if(bulkResponse.hasFailures()){
			logger.error("Bulk Message Post to ElasticSearch has error. Failure message is::" + bulkResponse.buildFailureMessage());
				int failedCount = 1;
				for (BulkItemResponse resp : bulkResponse) {
					logger.info("**** Failed Messages are: *****");	
				if (resp.isFailed()) {
					//Need to handle failure messages logging in a better way
					logger.info("Failed Message # " + failedCount + " is::" + resp.getFailure().getMessage());
					failedCount++;
					} else {
						//Do stats handling here
					}
				}
				
				int msgFailurePercentage = (Integer)((failedCount/offsetMsgMap.size()) * 100); 
				logger.info("% of failed message post to ElasticSearch is::" + msgFailurePercentage);
				logger.info("ElasticSearch msg failure tolerance % is::" + this.config.esMsgFailureTolerancePercent);
				if(msgFailurePercentage > this.config.esMsgFailureTolerancePercent){
					logger.error("% of failed messages is GREATER than set tolerance.Hence would return to consumer job with FALSE");
					return false;
				}
				else{
					logger.info("% of failed messages is LESSER than set tolerance.Hence would return to consumer job with TRUE");
					return true;
				}
					
				
			}
		logger.info("Bulk Post to ElasticSearch was success with no single error. Returning to consumer job with true.");
		return true;
	}

	public abstract void transformMessage() throws Exception;
	
	public abstract void prepareForPostToElasticSearch() throws Exception;

	
}
