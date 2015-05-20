package org.elasticsearch.kafka.consumer;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ConsumerJob {

	private ConsumerConfig consumerConfig;
	private long offsetForThisRound;
	private MessageHandler msgHandler;
	private Client esClient;
	public KafkaClient kafkaConsumerClient;
	private Long nextOffsetToProcess;
	private boolean isStartingFirstTime;
	private String consumerGroupTopicPartition;
	private ByteBufferMessageSet byteBufferMsgSet = null;
	private FetchResponse fetchResponse = null;

	Logger logger = ConsumerLogger.getLogger(this.getClass());

	private int kafkaIntSleepTime = 60000;
	private int esIntSleepTime = 60000;

	public ConsumerJob(ConsumerConfig config) throws Exception {
		this.consumerConfig = config;
		this.isStartingFirstTime = true;
		this.initKakfa();
		this.initElasticSearch();
		this.createMessageHandler();
	}

	void initElasticSearch() throws Exception {
		String[] esHostPortList = this.consumerConfig.esHostPortList.trim()
				.split(",");
		logger.info("ElasticSearch HostPortList is:: "
				+ this.consumerConfig.esHostPortList);
		logger.info("Initializing ElasticSearch");
		logger.info("esClusterName is::" + this.consumerConfig.esClusterName);

		try {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", this.consumerConfig.esClusterName)
					.build();
			for (String eachHostPort : esHostPortList) {
				logger.info("Setting the Elasticsearch client with :: "
						+ eachHostPort);
				this.esClient = new TransportClient(settings)
						.addTransportAddress(new InetSocketTransportAddress(
								eachHostPort.split(":")[0].trim(), Integer
										.parseInt(eachHostPort.split(":")[1]
												.trim())));
			}
			logger.info("Initializing ElasticSearch Success. ElasticSearch Client created and intialized.");
		} catch (Exception e) {
			logger.fatal("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::"
					+ e.getMessage());
			throw e;
		}
	}

	void initKakfa() throws Exception {
		logger.info("Initializing Kafka");
		String consumerGroupName = consumerConfig.consumerGroupName;
		if (consumerGroupName.isEmpty()) {
			consumerGroupName = "Client_" + consumerConfig.topic + "_"
					+ consumerConfig.partition;
			logger.info("ConsumerGroupName is empty.Hence created a group name");
		}
		logger.info("consumerGroupName is:" + consumerGroupName);
		this.consumerGroupTopicPartition = consumerGroupName + "_"
				+ consumerConfig.topic + "_" + consumerConfig.partition;
		logger.info("consumerGroupTopicPartition is:"
				+ consumerGroupTopicPartition);
		this.kafkaConsumerClient = new KafkaClient(consumerConfig,
				consumerConfig.zookeeper, consumerConfig.brokerHost,
				consumerConfig.brokerPort, consumerConfig.partition,
				consumerGroupName, consumerConfig.topic);
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
		logger.info("Kafka Reintialization Kafka & Consumer Client is success. Will sleep for "
				+ kafkaIntSleepTime / 1000 + " to allow kafka stabilize");
		Thread.sleep(kafkaIntSleepTime);
	}

	void reInitElasticSearch() throws Exception {
		logger.info("Re-Initializing ElasticSearch");
		logger.info("Closing ElasticSearch");
		this.esClient.close();
		logger.info("Completed closing ElasticSearch and starting to initialize again");
		this.initElasticSearch();
		logger.info("ReInitialized ElasticSearch. Will sleep for "
				+ esIntSleepTime / 1000);
		Thread.sleep(esIntSleepTime);
	}

	public void checkKafkaOffsets() {
		try {
			long currentOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
			long earliestOffset = kafkaConsumerClient.getEarliestOffset();
			long latestOffset = kafkaConsumerClient.getLastestOffset();
			logger.info("Kafka offsets: currentOffset=" + currentOffset
					+ "; earliestOffset=" + earliestOffset + "; latestOffset="
					+ latestOffset);
		} catch (Exception e) {
			logger.warn("Exception from checkKafkaOffsets(): " + e.getMessage(), e);
			e.printStackTrace();
		}

	}

	private void computeOffset() throws Exception {
		
		logger.info("**** Computing Kafka offset ***");
		logger.info("startOffsetFrom = " + consumerConfig.startOffsetFrom);
		if (consumerConfig.startOffsetFrom.equalsIgnoreCase("CUSTOM")) {
			if (consumerConfig.startOffset != -1) {
				offsetForThisRound = consumerConfig.startOffset;
			} else {
				throw new Exception(
					"Custom start offset for " + consumerGroupTopicPartition
					+ " is -1 which is not an acceptable value - please provide a valid offset; exiting");
			}
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("OLDEST")) {
			this.offsetForThisRound = kafkaConsumerClient.getEarliestOffset();
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("LATEST")) {
			offsetForThisRound = kafkaConsumerClient.getLastestOffset();
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("RESTART")) {
			logger.info("ReStarting from where the Offset is left for consumer: "
					+ consumerGroupTopicPartition);
			offsetForThisRound = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
			if (offsetForThisRound == -1)
			{
				// if this is the first time this client tried to read - offset might be -1
				// [ TODO figure out all cases when this can happen]
				// try to get the Earliest offset and read from there - it may lead
				// to processing events that may have already be processed - but it is safer than
				// starting from the Latest offset in case not all events were processed before				
				offsetForThisRound = kafkaConsumerClient.getEarliestOffset();
				logger.info("offsetForThisRound is set to the EarliestOffset since currentOffset is -1; offsetForThisRound=" + offsetForThisRound);
				// also store this as the CurrentOffset to Kafka - to avoid the multiple cycles through
				// this logic in the case no events are coming to the topic for a long time and
				// we always get currentOffset as -1 from Kafka
				try {
					kafkaConsumerClient.saveOffsetInKafka( offsetForThisRound, ErrorMapping.NoError());
				} catch (Exception e) {
					logger.fatal("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
					throw new Exception("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
				}

			} else {
				logger.info("offsetForThisRound is set to the CurrentOffset: " + offsetForThisRound);				
			}
		}
		long earliestOffset = kafkaConsumerClient.getEarliestOffset();
		// check for a corner case when the computed offset (either current or custom)
		// is less than the Earliest offset - which could happen if some messages were 
		// cleaned up from the topic/partition due to retention policy
		if (offsetForThisRound < earliestOffset){
			logger.warn("WARNING: computed offset (either current or custom) = " + offsetForThisRound +
					" is less than EarliestOffset = " + earliestOffset + 
					"; setting offsetForThisRound to the EarliestOffset");
			offsetForThisRound = earliestOffset;
			try {
				kafkaConsumerClient.saveOffsetInKafka( offsetForThisRound, ErrorMapping.NoError());
			} catch (Exception e) {
				logger.fatal("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
				throw new Exception("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
			}
		}
		logger.info("Resulting offsetForThisRound = " + offsetForThisRound);
	}

	private void createMessageHandler() throws Exception {
		try {
			logger.info("MessageHandler Class given in config is:"
					+ this.consumerConfig.messageHandlerClass);
			msgHandler = (MessageHandler) Class.forName(
					this.consumerConfig.messageHandlerClass).newInstance();
			msgHandler.initMessageHandler(esClient, consumerConfig);
			logger.debug("Created and initialized MessageHandler: "
					+ consumerConfig.messageHandlerClass);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;

		}
	}

	public void doRun() throws Exception {
		long jobStartTime = System.currentTimeMillis();
		boolean esPostResult = false;
		this.checkKafkaOffsets();
		if (!isStartingFirstTime) {
			// do not read offset from Kafka after each run - we just stored it there
			// If this is the only thread that is processing data from this partition - 
			// we can rely on the in-memory nextOffsetToProcess variable
			offsetForThisRound = nextOffsetToProcess;
		} else {
			// if this is the first time we run the Consumer - get it from Kafka
			try {
				computeOffset();
			} catch (Exception e) {
				logger.error("Exception getting Kafka offsets - exiting: ", e);
				// do not re-init Kafka here for now - re-introduce this once limited number of tries
				// is implemented - and when it will be clear that re-init-ing of KAfka actually worked
				// reInitKakfa();
				throw e;
			}
			// mark this as not first time startup anymore - since we already saved correct offset
			// to Kafka, and to avoid going through the logic of figuring out the initial offset
			// every round if it so happens that there were no events from Kafka for a long time
			isStartingFirstTime = false;
			nextOffsetToProcess = offsetForThisRound;
		}

		fetchResponse = kafkaConsumerClient.getMessagesFromKafka( offsetForThisRound, consumerConfig.bulkSize);
		long timeAfterKafaFetch = System.currentTimeMillis();
		logger.info("Fetched the reponse from Kafka. Approx time taken is :: "
				+ (timeAfterKafaFetch - jobStartTime) + " milliSec");
		if (fetchResponse.hasError()) {
			// Do things according to the error code
			handleError();
			return;
		}
		
		// TODO handle failure here
		byteBufferMsgSet = kafkaConsumerClient.fetchMessageSet(fetchResponse);
		long timeAftKafaFetchMsgSet = System.currentTimeMillis();
		logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is :: "
				+ (timeAftKafaFetchMsgSet - timeAfterKafaFetch) + " milliSec");
		if (byteBufferMsgSet.validBytes() <= 0) {
			logger.warn("No events were read from Kafka - finishing this round of reads from Kafka");
			//Thread.sleep(1000);
			// TODO re-review this logic
			long latestOffset = kafkaConsumerClient.getLastestOffset();
			if (latestOffset != offsetForThisRound) {
				logger.warn("latestOffset [" + latestOffset + 
						"] is not the same as the current offsetForThisRound for this run [" + offsetForThisRound 
						+ "] - committing latestOffset to Kafka");
				try {
					kafkaConsumerClient.saveOffsetInKafka(
						latestOffset, 
						fetchResponse.errorCode(consumerConfig.topic, consumerConfig.partition));
				} catch (Exception e) {
					// throw an exception as this will break reading messages in the next round
					logger.fatal("Failed to commit the offset in Kafka - exiting: " + e.getMessage(), e);
					throw e;
				}
			}
			return;
		}
		logger.info("Starting to prepare ElasticSearch");
		nextOffsetToProcess = msgHandler.prepareForPostToElasticSearch(byteBufferMsgSet.iterator());
		
		long timeAtPrepareES = System.currentTimeMillis();
		//logger.debug("Completed preparing ElasticSearch.Approx time taken to initMsg,TransformMsg,Prepare ES is::"
		//		+ (timeAtPrepareES - timeAftKafkaMsgCollate) + " milliSec");
		//logger.debug("nextOffsetToProcess is:: "
		//		+ nextOffsetToProcess
		//		+ " This is the offset that will be committed once elasticSearch post is complete");

		if (Boolean.parseBoolean(this.consumerConfig.isDryRun.trim())) {
			logger.info("**** This is a dry run, hence NOT committing the offset in Kafka ****");
			return;
		}
		try {
			logger.info("posting the messages to ElasticSearch");
			esPostResult = msgHandler.postToElasticSearch();
		} catch (ElasticsearchException esE) {
			logger.fatal("ElasticsearchException exception happened. Detailed Message is:: "
					+ esE.getDetailedMessage());
			logger.fatal("Root Cause:: " + esE.getRootCause());
			logger.info("Will try reinitializing ElasticSearch now");
			this.reInitElasticSearch();
			logger.info("Tried reinitializing ElasticSearch, returning back");
			return;
		}

		long timeAftEsPost = System.currentTimeMillis();
		logger.info("Approx time it took to post of ElasticSearch is:"
				+ (timeAftEsPost - timeAtPrepareES) + " milliSec");

		if (!esPostResult) {
			logger.info("The ES Post failed but we still commit the offset to Kafka - to avoid re-rpocessing the same messages forever");
		} else {
			logger.info("The ES Post is success and this is not a dry run and hence commiting the offset to Kafka");
		}
		logger.info("Commiting offset #:: " + this.nextOffsetToProcess);
		// TODO optimize getting of the fetchResponse.errorCode - in some cases there is no error, 
		// so no need to call the API every time
		try {
			this.kafkaConsumerClient.saveOffsetInKafka(
				this.nextOffsetToProcess, fetchResponse.errorCode(
					this.consumerConfig.topic,
					this.consumerConfig.partition));
		} catch (Exception e) {
			logger.fatal("Failed to commit the Offset in Kafka after processing and posting to ES: ", e);
			logger.info("Trying to reInitialize Kafka and commit the offset again...");
			this.reInitKakfa();
			try {
				logger.info("Attempting to commit the offset after reInitializing Kafka now..");
				this.kafkaConsumerClient.saveOffsetInKafka(
					this.nextOffsetToProcess, fetchResponse.errorCode(
						this.consumerConfig.topic,
						this.consumerConfig.partition));
			} catch (Exception e2) {
				// KrishnaRaj - Need to handle this situation where
				// committing offset back to Kafka is failing even after
				// reInitializing kafka.
				logger.fatal("Failed to commit the Offset in Kafka even after reInitializing Kafka - exiting: " +
				 e2.getMessage(), e2);
				// there is no point in continuing  - as we will keep re-processing events
				// from the old offset. Throw an exception and exit;
				// manually fix KAfka/Zookeeper env and re-start from a 
				// desired , possibly custom, offset
				throw e2;
			}
		}

		long timeAtEndOfJob = System.currentTimeMillis();
		logger.info("*** This round of ConsumerJob took approx:: "
				+ (timeAtEndOfJob - jobStartTime) + " milliSec."
				+ "Messages from Offset:" + this.offsetForThisRound + " to "
				+ this.nextOffsetToProcess
				+ " were processed in this round. ****");
		this.byteBufferMsgSet.buffer().clear();
		this.byteBufferMsgSet = null;
		this.fetchResponse = null;
	}

	public void handleError() throws Exception {
		// Do things according to the error code
		short errorCode = fetchResponse.errorCode(
				consumerConfig.topic, consumerConfig.partition);
		logger.error("Error fetching events from Kafka - handling it. Error code: "
				+ errorCode);
		if (errorCode == ErrorMapping.BrokerNotAvailableCode()) {
			logger.error("BrokerNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
			reInitKakfa();
		} else if (errorCode == ErrorMapping.InvalidFetchSizeCode()) {
			logger.error("InvalidFetchSizeCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
			reInitKakfa();
		} else if (errorCode == ErrorMapping.InvalidMessageCode()) {
			logger.error("InvalidMessageCode error happened when fetching message from Kafka, not handling it. Returning.");
		} else if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
			logger.error("LeaderNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
			reInitKakfa();
		} else if (errorCode == ErrorMapping.MessageSizeTooLargeCode()) {
			logger.error("MessageSizeTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
		} else if (errorCode == ErrorMapping.NotLeaderForPartitionCode()) {
			logger.error("NotLeaderForPartitionCode error happened when fetching message from Kafka, not handling it. ReInitiating Kafka Client.");
			reInitKakfa();
		} else if (errorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
			logger.error("OffsetMetadataTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
		} else if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
			logger.error("OffsetOutOfRangeCode error happened when fetching message from Kafka");
			// It is better not to try to fix this issue programmatically: if the offset is wrong,
			// either this is the first time we read form Kafka or not - it is better to figure out 
			// why it is wrong and fix the corresponding logic or CUSTOM offset, 
			// rather than blindly reset it to the Latest offset
			/*
			if (isStartingFirstTime) {
				logger.info("Handling OffsetOutOfRange error: This is 1st round of consumer, hence setting the StartOffsetFrom = LATEST. This will ensure that the latest offset is picked up in next try");
				consumerConfig.setStartOffsetFrom("LATEST");
			} else {
				logger.info("Handling OffsetOutOfRange error: This is not the 1st round of consumer, hence will get the latest offset and setting it as offsetForThisRound and will read from latest");
				long latestOffset = kafkaConsumerClient.getLastestOffset();
				logger.info("Handling OffsetOutOfRange error. Will try to read from the LatestOffset = "
						+ latestOffset);
				offsetForThisRound = latestOffset;
			}
			/*  */
			return;

		} else if (errorCode == ErrorMapping.ReplicaNotAvailableCode()) {
			logger.error("ReplicaNotAvailableCode error happened when fetching message from Kafka, not handling it. Returning.");
			reInitKakfa();
			return;

		} else if (errorCode == ErrorMapping.RequestTimedOutCode()) {
			logger.error("RequestTimedOutCode error happened when fetching message from Kafka, not handling it. Returning.");
			reInitKakfa();
			return;

		} else if (errorCode == ErrorMapping.StaleControllerEpochCode()) {
			logger.error("StaleControllerEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
			return;

		} else if (errorCode == ErrorMapping.StaleLeaderEpochCode()) {
			logger.error("StaleLeaderEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
			return;

		} else if (errorCode == ErrorMapping.UnknownCode()) {
			logger.error("UnknownCode error happened when fetching message from Kafka, not handling it. Returning.");
			reInitKakfa();
			return;

		} else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
			logger.error("UnknownTopicOrPartitionCode error happened when fetching message from Kafka, not handling it. Returning.");
			reInitKakfa();
			return;

		}

	}
	public void stop() {
		logger.info("About to close the Kafka & ElasticSearch Client");
		this.kafkaConsumerClient.close();
		this.esClient.close();
		logger.info("Closed Kafka & ElasticSearch Client");
	}

}
