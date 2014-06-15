package org.elasticsearch.kafka.consumer;

import java.util.Iterator;
import java.util.LinkedHashMap;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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
	Logger logger = ConsumerLogger.getLogger(this.getClass());
		
	public ConsumerJob(ConsumerConfig config) throws Exception {
		this.consumerConfig = config;
		this.isStartingFirstTime = true;
		this.initKakfa();
		this.initElasticSearch();
		this.createMessageHandler();
	}

	void initElasticSearch() throws Exception{
		logger.info("Initializing ElasticSearch");
		logger.info("esClusterName is::" + this.consumerConfig.esClusterName);
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", this.consumerConfig.esClusterName).build();
		try{
			this.esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(this.consumerConfig.esHost, this.consumerConfig.esPort));
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
		this.kafkaConsumerClient.findNewLeader();
		logger.info("Found new leader in Kafka broker");
		this.kafkaConsumerClient.initConsumer();
		logger.info("Kafka Reintialization Kafka & Consumer Client is success");
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
		this.isStartingFirstTime = false;
		logger.info("computedOffset:=" + this.readOffset);
		System.out.println("readOffset:=" + this.readOffset);		
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
		LinkedHashMap<Long,Message> offsetMsgMap = new  LinkedHashMap<Long,Message>();
		computeOffset();
		FetchResponse fetchResponse = this.kafkaConsumerClient.getFetchResponse(this.readOffset, this.consumerConfig.bulkSize);
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
				logger.error("OffsetOutOfRangeCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.ReplicaNotAvailableCode()){
				logger.error("ReplicaNotAvailableCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;
				
			}
			else if(errorCode == ErrorMapping.RequestTimedOutCode()){
				logger.error("RequestTimedOutCode error happened when fetching message from Kafka, not handling it. Returning.");
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
				return;	
				
			}
			else if(errorCode == ErrorMapping.UnknownTopicOrPartitionCode()){
				logger.error("UnknownTopicOrPartitionCode error happened when fetching message from Kafka, not handling it. Returning.");
				return;	
				
			}
			return;
		}
		ByteBufferMessageSet byteBufferMsgSet = this.kafkaConsumerClient.fetchMessageSet(fetchResponse);
		long timeAftKafaFetchMsgSet = System.currentTimeMillis();
		logger.info("Completed MsgSet fetch from Kafka. Approx time taken is::" + (timeAftKafaFetchMsgSet-timeAfterKafaFetch) + " milliSec");
		if(byteBufferMsgSet.validBytes() <= 0){
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
		}
		long timeAftKafkaMsgCollate = System.currentTimeMillis();
		logger.info("Completed collating the Messages and Offset into Map. Approx time taken is::" + (timeAftKafkaMsgCollate-timeAftKafaFetchMsgSet) + " milliSec");
		//this.msgHandler.initMessageHandler();
		//logger.info("Initialized Message handler");
		this.msgHandler.setOffsetMsgMap(offsetMsgMap);
		logger.info("Starting to transform the messages");
		this.msgHandler.transformMessage();
		logger.info("Completed transforming messages");
		logger.info("Starting to prepare ElasticSearch");
		this.msgHandler.prepareForPostToElasticSearch();
		long timeAtPrepareES = System.currentTimeMillis();
		logger.info("Completed preparing ElasticSearch.Approx time taken to initMsg,TransformMsg,Prepare ES is::" + (timeAtPrepareES-timeAftKafkaMsgCollate) + " milliSec");
		logger.info("nextOffsetToProcess is::" + nextOffsetToProcess + "This is the offset that will be commited once elasticSearch post is complete");
		boolean esPostResult = this.msgHandler.postToElasticSearch();
		long timeAftEsPost   = System.currentTimeMillis();
		logger.info("Approx time it took to post of ElasticSearch is:" + (timeAftEsPost-timeAtPrepareES) + " milliSec");
		if(esPostResult){
			logger.info("Commiting offset #::" + this.nextOffsetToProcess);
			this.kafkaConsumerClient.saveOffsetInKafka(this.nextOffsetToProcess, fetchResponse.errorCode(this.consumerConfig.topic, this.consumerConfig.partition));
		}
		else{
			logger.error("Posting to ElasticSearch has error");
			//Need to decide about what to do when message failure rate is > set messageFailuresTolerance. For now still commiting the  
		}
		long timeAtEndOfJob  = System.currentTimeMillis();
		logger.info("*** This round of ConsumerJob took approx:: " + (timeAtEndOfJob-jobStartTime) + " milliSec." + "Messages from Offset:" + this.readOffset + " to " + this.currentOffsetAtProcess + " were processed in this round. ****");
	}
	
	
	public void stop(){
		logger.info("About to close the Kafka & ElasticSearch Client");
		this.kafkaConsumerClient.close();
		this.esClient.close();
		logger.info("Closed Kafka & ElasticSearch Client");
	}
	
}


