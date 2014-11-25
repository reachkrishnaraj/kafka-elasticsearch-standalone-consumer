package org.elasticsearch.kafka.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.consumer.helpers.ExceptionHelper;

public class ConsumerJob {

	private ConsumerConfig consumerConfig;
	private long readOffset;
	private MessageHandler msgHandler;
	private Client esClient;
	public KafkaClient kafkaConsumerClient;
	private Long currentOffsetAtProcess;
	private Long nextOffsetToProcess;
	//StatsReporter statsd;
	//private long statsLastPrintTime;
	//private Stats stats = new Stats();
	private boolean isStartingFirstTime;
	private String consumerGroupTopicPartition;
	private LinkedHashMap<Long,Message> offsetMsgMap = new  LinkedHashMap<Long,Message>();
	private ByteBufferMessageSet byteBufferMsgSet = null;
	private FetchResponse fetchResponse = null;

	Logger logger = ConsumerLogger.getLogger(this.getClass());
	
	private int kafkaIntSleepTime = 60000;
	private int esIntSleepTime = 60000;
		
	public LinkedHashMap<Long, Message> getOffsetMsgMap() {
		return offsetMsgMap;
	}

	public void setOffsetMsgMap(LinkedHashMap<Long, Message> offsetMsgMap) {
		this.offsetMsgMap = offsetMsgMap;
	}

	public ConsumerJob(ConsumerConfig config) throws Exception {
		this.consumerConfig = config;
		this.isStartingFirstTime = true;
		this.initKakfa();
		this.initElasticSearch();
		this.createMessageHandler();
	}

	void initElasticSearch() throws Exception{
		String[] esHostPortList = this.consumerConfig.esHostPortList.trim().split(",");
		logger.info("ElasticSearch HostPortList is:: " + this.consumerConfig.esHostPortList);
		logger.info("Initializing ElasticSearch");
		logger.info("esClusterName is::" + this.consumerConfig.esClusterName);
		
		try{
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", this.consumerConfig.esClusterName).build();
			for(String eachHostPort : esHostPortList){
				logger.info("Setting the Elasticsearch client with :: " + eachHostPort);
				this.esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(eachHostPort.split(":")[0].trim(), Integer.parseInt(eachHostPort.split(":")[1].trim())));
			}
			//this.esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(this.consumerConfig.esHost, this.consumerConfig.esPort));
			logger.info("Initializing ElasticSearch Success. ElasticSearch Client created and intialized.");
		}
		catch(Exception e){
			logger.fatal("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::" + e.getMessage());
			throw e;
		}
	}
	
	void initKakfa() throws Exception {
		logger.info("Initializing Kafka");
		String consumerGroupName = consumerConfig.consumerGroupName;
		if(consumerGroupName.isEmpty()){
			consumerGroupName = "Client_" + consumerConfig.topic + "_" + consumerConfig.partition;
			logger.info("ConsumerGroupName is empty.Hence created a group name");
		}
		logger.info("consumerGroupName is:" + consumerGroupName);
		this.consumerGroupTopicPartition = consumerGroupName + "_" +  consumerConfig.topic + "_" + consumerConfig.partition;
		logger.info("consumerGroupTopicPartition is:" + consumerGroupTopicPartition);
		this.kafkaConsumerClient = new KafkaClient(consumerConfig, consumerConfig.zookeeper, consumerConfig.brokerHost, consumerConfig.brokerPort, consumerConfig.partition, consumerGroupName, consumerConfig.topic);
		logger.info("Kafka intialization success and kafka client created and intialized");
		
	}
	
	
	void reInitKakfa() throws Exception {
		logger.info("Kafka Reintialization Kafka & Consumer Client");
		this.kafkaConsumerClient.close();
		logger.info("Kafka client closed");
		logger.info("Connecting to zookeeper again");
		this.kafkaConsumerClient.connectToZooKeeper();
		logger.info("Completed connecting to zookeeper and finding the new leader now.");
		this.kafkaConsumerClient.findNewLeader();
		logger.info("Found new leader in Kafka broker. Now, initializing the kafka consumer");
		this.kafkaConsumerClient.initConsumer();
		logger.info("Kafka Reintialization Kafka & Consumer Client is success. Will sleep for " + kafkaIntSleepTime/1000 + " to allow kafka stabilize");
		Thread.sleep(kafkaIntSleepTime);
	}
	
	void reInitElasticSearch() throws Exception{
		logger.info("Re-Initializing ElasticSearch");
		logger.info("Closing ElasticSearch");
		this.esClient.close();
		logger.info("Completed closing ElasticSearch and starting to initialize again");
		this.initElasticSearch();
		logger.info("ReInitialized ElasticSearch. Will sleep for " + esIntSleepTime/1000);
		Thread.sleep(esIntSleepTime);
	}
	
	void computeOffset() throws Exception{
		if(!isStartingFirstTime){
			logger.info("This is not 1st time read in Kafka");
			this.readOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();			
			logger.info("computedOffset:=" + this.readOffset);
			return;
		}
		logger.info("**** Starting the Kafka Read for 1st time ***");
		if(consumerConfig.startOffsetFrom.equalsIgnoreCase("CUSTOM")){
			logger.info("startOffsetFrom is CUSTOM");
			logger.info("startOffset is given as:" + consumerConfig.startOffset);
			if(consumerConfig.startOffset != -1){
				this.readOffset = consumerConfig.startOffset;
			}
			else{
				logger.fatal("Custom start from Offset for" + this.consumerGroupTopicPartition + " is -1 which is a not acceptable value. When startOffsetFrom config value is custom, then a valid startOffset has to be provided. Exiting !");
				throw new RuntimeException("Custom start from Offset for" + this.consumerGroupTopicPartition + " is -1 which is a not acceptable value. When startOffsetFrom config value is custom, then a valid startOffset has to be provided");
			}	
		}
		else if(consumerConfig.startOffsetFrom.equalsIgnoreCase("OLDEST")){
			logger.info("startOffsetFrom is selected as OLDEST");
			this.readOffset = kafkaConsumerClient.getOldestOffset();
		}
		else if(consumerConfig.startOffsetFrom.equalsIgnoreCase("LATEST")){
			logger.info("startOffsetFrom is selected as LATEST");
			this.readOffset = kafkaConsumerClient.getLastestOffset();
		}
		else if(consumerConfig.startOffsetFrom.equalsIgnoreCase("RESTART")){
			logger.info("ReStarting from where the Offset is left for consumer:" + this.consumerGroupTopicPartition);
			logger.info("startOffsetFrom is selected as RESTART");
			this.readOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
		}
		//Moved this step which decides when to move to next round. The job will move to 2nd & subsequent rounds only after confirming that Post to ES is success for the 1st round.
		//This check is need becoz the starting point of the offset comes from the config and I want to be sure that starting offset from config is respected, commited and then move to 2nd & subsequent rounds. 
		//this.isStartingFirstTime = false;
		logger.info("computedOffset:=" + this.readOffset);
		//System.out.println("readOffset:=" + this.readOffset);		
	}
	
	
	private void createMessageHandler() throws Exception {
		try{
			logger.info("MessageHandler Class given in config is:" + this.consumerConfig.messageHandlerClass);
			this.msgHandler = (MessageHandler)Class.forName(this.consumerConfig.messageHandlerClass).newInstance();
			this.msgHandler.initMessageHandler(this.esClient, this.consumerConfig);
			logger.info("Created an initialized MessageHandle::" + this.consumerConfig.messageHandlerClass);
		}
		catch(Exception e){
			e.printStackTrace();
			throw e;
			
		}
	}
	
	public void doRun() throws Exception {
		logger.info("***** Starting a new round of kafka read and post to ElasticSearch ******");
		long jobStartTime = System.currentTimeMillis();
		boolean esPostResult = false;
		this.offsetMsgMap.clear();
		try{
			computeOffset();
		}
		catch(Exception e){
			logger.error("Exception when trying to compute offset. Mostly because Kafka is not connetable. So, will try Reintializate kafka now ...");
			reInitKakfa();
		}
		
		this.fetchResponse = this.kafkaConsumerClient.getFetchResponse(this.readOffset, this.consumerConfig.bulkSize);
		long timeAfterKafaFetch = System.currentTimeMillis();
		logger.info("Fetched the reponse from Kafka. Approx time taken is::" + (timeAfterKafaFetch-jobStartTime) + " milliSec");
		if(fetchResponse.hasError()){			
			//Do things according to the error code
			short errorCode = fetchResponse.errorCode(this.consumerConfig.topic, this.consumerConfig.partition);
			logger.error("Kafka fetch error happened. Error code is::" + errorCode + " Starting to handle the error");
			if (errorCode == ErrorMapping.BrokerNotAvailableCode()){
				logger.error("BrokerNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
				this.reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.InvalidFetchSizeCode()){
				logger.error("InvalidFetchSizeCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
				reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.InvalidMessageCode()){
				logger.error("InvalidMessageCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.LeaderNotAvailableCode()){
				logger.error("LeaderNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
				reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.MessageSizeTooLargeCode()){
				logger.error("MessageSizeTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.NotLeaderForPartitionCode()){
				logger.error("NotLeaderForPartitionCode error happened when fetching message from Kafka, not handling it. ReInitiating Kafka Client.");
				reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.OffsetMetadataTooLargeCode()){
				logger.error("OffsetMetadataTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.OffsetOutOfRangeCode()){
				logger.error("OffsetOutOfRangeCode error happened when fetching message from Kafka");
				if(isStartingFirstTime){
					logger.info("Handling OffsetOutOfRange error: This is 1st round of consumer, hence setting the StartOffsetFrom = LATEST. This will ensure that the latest offset is picked up in next try");
					consumerConfig.setStartOffsetFrom("LATEST");
				}
				else{
					logger.info("Handling OffsetOutOfRange error: Thi is not the 1st round of consumer, hence will get the latest offset and setting it as readOffset and will read from latest");
					long latestOffset = kafkaConsumerClient.getLastestOffset();
					logger.info("Handling OffsetOutOfRange error: Got the latest offset and it is::" + latestOffset + ". Will try to read from " + latestOffset + " offset");
					this.readOffset = latestOffset;
				}
				return;
				
			}
			else if(errorCode == ErrorMapping.ReplicaNotAvailableCode()){
				logger.error("ReplicaNotAvailableCode error happened when fetching message from Kafka, not handling it. Returning.");
				reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.RequestTimedOutCode()){
				logger.error("RequestTimedOutCode error happened when fetching message from Kafka, not handling it. Returning.");
				reInitKakfa();
				return;
				
			}
			else if(errorCode == ErrorMapping.StaleControllerEpochCode()){
				logger.error("StaleControllerEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.StaleLeaderEpochCode()){
				logger.error("StaleLeaderEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;	
				
			}
			else if(errorCode == ErrorMapping.UnknownCode()){
				logger.error("UnknownCode error happened when fetching message from Kafka, not handling it. Returning.");
				reInitKakfa();
				return;	
				
			}
			else if(errorCode == ErrorMapping.UnknownTopicOrPartitionCode()){
				logger.error("UnknownTopicOrPartitionCode error happened when fetching message from Kafka, not handling it. Returning.");
				reInitKakfa();
				return;	
				
			}
			return;
		}
		this.byteBufferMsgSet = this.kafkaConsumerClient.fetchMessageSet(fetchResponse);
		long timeAftKafaFetchMsgSet = System.currentTimeMillis();
		logger.info("Completed MsgSet fetch from Kafka. Approx time taken is::" + (timeAftKafaFetchMsgSet-timeAfterKafaFetch) + " milliSec");
		if(this.byteBufferMsgSet.validBytes() <= 0){
			logger.warn("No valid bytes available in Kafka response MessageSet. Sleeping for 1 second");
			Thread.sleep(1000);
			return;
		}
		Iterator<MessageAndOffset> msgOffSetIter = byteBufferMsgSet.iterator();
		while(msgOffSetIter.hasNext()){
			MessageAndOffset msgAndOffset = msgOffSetIter.next();
			this.currentOffsetAtProcess = msgAndOffset.offset();
			this.nextOffsetToProcess = msgAndOffset.nextOffset();
			offsetMsgMap.put(currentOffsetAtProcess, msgAndOffset.message());
			//msgOffSetIter.remove();
		}
		long timeAftKafkaMsgCollate = System.currentTimeMillis();
		logger.info("Completed collating the Messages and Offset into Map. Approx time taken is::" + (timeAftKafkaMsgCollate-timeAftKafaFetchMsgSet) + " milliSec");
		//this.msgHandler.initMessageHandler();
		//logger.info("Initialized Message handler");
		this.msgHandler.setOffsetMsgMap(this.offsetMsgMap);
		System.out.println(this.msgHandler.getOffsetMsgMap().size());
		logger.info("# of message available for this round is:" + this.offsetMsgMap.size());
		logger.info("Starting to transform the messages");
		this.msgHandler.transformMessage();
		logger.info("# of messages which failed during transforming:: " + this.msgHandler.getOffsetFailedMsgMap().size());
		logger.info("Completed transforming messages");
		logger.info("Starting to prepare ElasticSearch");
		this.msgHandler.prepareForPostToElasticSearch();
		long timeAtPrepareES = System.currentTimeMillis();
		logger.info("Completed preparing ElasticSearch.Approx time taken to initMsg,TransformMsg,Prepare ES is::" + (timeAtPrepareES-timeAftKafkaMsgCollate) + " milliSec");
		logger.info("nextOffsetToProcess is::" + nextOffsetToProcess + "This is the offset that will be commited once elasticSearch post is complete");
		
		try{
			if(Boolean.parseBoolean(this.consumerConfig.isDryRun.trim())){
				logger.info("****** This is a dry run, hence NOT posting the messages to ElasticSearch AND the Kafka Offset wont be commited ***** ");
			}
			else{
				logger.info("Not a dry run, hence posting the messages to ElasticSearch");
				esPostResult = this.msgHandler.postToElasticSearch();
			}	
		}
		catch(ElasticsearchException esE){
			logger.fatal("ElasticsearchException exception happened. Detailed Message is:: " + esE.getDetailedMessage());
			logger.fatal("Root Cause::" + esE.getRootCause());
			logger.info("Will try reinitializing ElasticSearch now");
			this.reInitElasticSearch();
			logger.info("Tried reinitializing ElasticSearch, returning back");
			return;
		}
		
		long timeAftEsPost   = System.currentTimeMillis();
		logger.info("Approx time it took to post of ElasticSearch is:" + (timeAftEsPost-timeAtPrepareES) + " milliSec");
		
		if(Boolean.parseBoolean(this.consumerConfig.isDryRun.trim())){
			logger.info("**** This is a dry run, hence NOT committing the offset in Kafka ****");
		}
		else{
			logger.info("**** This is a NOT dry run ****");
			if(esPostResult){
				logger.info("The ES Post is success and this is not a dry run and hence commiting the offset to Kafka");
				logger.info("Commiting offset #::" + this.nextOffsetToProcess);
				try{
					this.kafkaConsumerClient.saveOffsetInKafka(this.nextOffsetToProcess, fetchResponse.errorCode(this.consumerConfig.topic, this.consumerConfig.partition));
				}
				catch(Exception e){
					logger.fatal("Failed to commit the Offset in Kafka after processing and posting to ES. The error stacktrace is below:");
					logger.fatal(ExceptionHelper.getStrackTraceAsString(e));
					logger.info("Trying to reInitialize Kafka and again try commiting the offset");
					this.reInitKakfa();
					try{
						logger.info("Attempting to commit the offset after reInitializing Kafka now..");
						this.kafkaConsumerClient.saveOffsetInKafka(this.nextOffsetToProcess, fetchResponse.errorCode(this.consumerConfig.topic, this.consumerConfig.partition));
					}
					catch(Exception exc){
						//KrishnaRaj - Need to handle this situation where committing offset back to Kafka is failing even after reInitializing kafka.
						logger.fatal("Failed to commit the Offset in Kafka even after reInitializing Kafka.");
					}
				}
				
				logger.info("Checking whether this round is the 1st round of the job...");
				if(this.isStartingFirstTime){
					logger.info("Yes, this is the first round of the job. Since the very 1st complete cycle is successfull, moving to next round and making the 'isStartingFirstTime' as false");
					this.isStartingFirstTime = false;
				}	
			}
			else{
				logger.error("*** This is not a dry run, but posting to ElasticSearch has issues, hence, not commiting the offset to Kafka. This error condition is still needed to be handled in the code.***");	
				logger.info("Starting Reinitializing ElasticSearch");
				this.reInitElasticSearch();
				logger.info("Reinitializing ElasticSearch Completed");
			}
		}
		
		long timeAtEndOfJob  = System.currentTimeMillis();
		logger.info("*** This round of ConsumerJob took approx:: " + (timeAtEndOfJob-jobStartTime) + " milliSec." + "Messages from Offset:" + this.readOffset + " to " + this.currentOffsetAtProcess + " were processed in this round. ****");
		this.byteBufferMsgSet.buffer().clear();
		this.byteBufferMsgSet = null;
		this.fetchResponse = null;
		System.gc();
		logger.info("** Starting to sleep for " + this.consumerConfig.consumerSleepTime + " seconds before starting the next round of consumer read and post **");
		Thread.sleep(this.consumerConfig.consumerSleepTime * 1000);
		logger.info("** Completed the sleep, will start the next round now **");
			
	}
	
	public void stop(){
		logger.info("About to close the Kafka & ElasticSearch Client");
		this.kafkaConsumerClient.close();
		this.esClient.close();
		logger.info("Closed Kafka & ElasticSearch Client");
	}
	
}


