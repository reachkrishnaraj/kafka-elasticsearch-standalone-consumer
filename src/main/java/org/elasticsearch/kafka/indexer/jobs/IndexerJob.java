package org.elasticsearch.kafka.indexer.jobs;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.KafkaClient;
import org.elasticsearch.kafka.indexer.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class IndexerJob implements Callable<IndexerJobStatus> {

	private static final Logger logger = LoggerFactory.getLogger(IndexerJob.class);
	private ConsumerConfig consumerConfig;
	private MessageHandler msgHandler;
	private TransportClient esClient;
	public KafkaClient kafkaConsumerClient;
	private long offsetForThisRound;
	private long nextOffsetToProcess;
	private boolean isStartingFirstTime;
	private ByteBufferMessageSet byteBufferMsgSet = null;
	private FetchResponse fetchResponse = null;
	private final String currentTopic;
	private final int currentPartition;

	private int kafkaReinitSleepTimeMs = 6000;
    private IndexerJobStatus indexerJobStatus;
    private volatile boolean shutdownRequested = false;
    private boolean isDryRun = false;

	public IndexerJob(ConsumerConfig config, int partition) throws Exception {
		this.consumerConfig = config;
		this.currentPartition = partition;
		this.currentTopic = config.topic;
		indexerJobStatus = new IndexerJobStatus(-1L, IndexerJobStatusEnum.Created, partition);
		isStartingFirstTime = true;
		isDryRun = Boolean.parseBoolean(config.isDryRun);
		initElasticSearch();
		initKafka();
		createMessageHandler();
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Initialized);
	}

	void initKafka() throws Exception {
		logger.info("Initializing Kafka for partition {}...",currentPartition);
		String consumerGroupName = consumerConfig.consumerGroupName;
		if (consumerGroupName.isEmpty()) {
			consumerGroupName = "es_indexer_" + currentTopic + "_" + currentPartition;
			logger.info("ConsumerGroupName was empty, set it to {} for partition {}", consumerGroupName,currentPartition);
		}
		String kafkaClientId = consumerGroupName  + "_" + currentPartition;
		logger.info("kafkaClientId={} for partition {}", kafkaClientId,currentPartition);
		kafkaConsumerClient = new KafkaClient(consumerConfig, kafkaClientId, currentPartition);
		logger.info("Kafka client created and intialized OK for partition {}",currentPartition);
	}

	private void initElasticSearch() throws Exception {
		String[] esHostPortList = consumerConfig.esHostPortList.trim().split(",");
		logger.info("Initializing ElasticSearch... hostPortList={}, esClusterName={} for partition {}",
				consumerConfig.esHostPortList, consumerConfig.esClusterName,currentPartition);

		// TODO add validation of host:port syntax - to avoid Runtime exceptions
		try {
			Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", consumerConfig.esClusterName)
				.build();
			esClient = new TransportClient(settings);
			for (String eachHostPort : esHostPortList) {
				logger.info("adding [{}] to TransportClient for partition {}... ", eachHostPort,currentPartition);
				esClient.addTransportAddress(
					new InetSocketTransportAddress(
						eachHostPort.split(":")[0].trim(), 
						Integer.parseInt(eachHostPort.split(":")[1].trim())
					)
				);
			}
			logger.info("ElasticSearch Client created and intialized OK for partition {}",currentPartition);
		} catch (Exception e) {
			logger.error("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::"
					+ e.getMessage());
			throw e;
		}
	}

	void reInitKafka() throws Exception {
		logger.info("Re-initializing Kafka for partition {}...",currentPartition);
		kafkaConsumerClient.close();
		logger.info("Kafka client closed for partition {}",currentPartition);
		logger.info("Connecting to zookeeper again for partition {}",currentPartition);
		kafkaConsumerClient.connectToZooKeeper();
		kafkaConsumerClient.findNewLeader();
		kafkaConsumerClient.initConsumer();
		logger.info("Kafka Reintialization is successful. Will sleep for {} ms to allow kafka to stabilize for partition {}"
				, kafkaReinitSleepTimeMs,currentPartition);
		Thread.sleep(kafkaReinitSleepTimeMs);
	}

	private void createMessageHandler() throws Exception {
		try {
			logger.info("MessageHandler Class given in config is {} for partition {}", consumerConfig.messageHandlerClass,currentPartition);
			msgHandler = (MessageHandler) Class
					.forName(consumerConfig.messageHandlerClass)
					.getConstructor(TransportClient.class, ConsumerConfig.class)
					.newInstance(esClient, consumerConfig);
			logger.debug("Created and initialized MessageHandler: {} for partition {}", consumerConfig.messageHandlerClass,currentPartition);
		} catch (Exception e) {
			logger.error("Exception creating MessageHandler class for partition {}: ",currentPartition, e);
			throw e;
		}
	}

	// a hook to be used by the Manager app to request a graceful shutdown of the job
	public void requestShutdown() {
		shutdownRequested = true;
	}

	public void checkKafkaOffsets() {
		try {
			long currentOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
			long earliestOffset = kafkaConsumerClient.getEarliestOffset();
			long latestOffset = kafkaConsumerClient.getLastestOffset();
			logger.info("Kafka offsets: currentOffset={}; earliestOffset={}; latestOffset={} for partition {}",
					currentOffset, earliestOffset, latestOffset,currentPartition);
		} catch (Exception e) {
			logger.warn("Exception from checkKafkaOffsets(): for partition {}" ,currentPartition, e);
			e.printStackTrace();
		}

	}

	private void computeOffset() throws Exception {
		logger.info("**** Computing Kafka offset *** for partition {}",currentPartition);
		logger.info("startOffsetFrom={} for partition {}", consumerConfig.startOffsetFrom,currentPartition);
		if (consumerConfig.startOffsetFrom.equalsIgnoreCase("CUSTOM")) {
			if (consumerConfig.startOffset >= 0) {
				offsetForThisRound = consumerConfig.startOffset;
			} else {
				throw new Exception(
						"Custom start offset for topic [" + currentTopic + "], partition [" +
								currentPartition + "] is < 0, which is not an acceptable value - please provide a valid offset; exiting");
			}
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("EARLIEST")) {
			this.offsetForThisRound = kafkaConsumerClient.getEarliestOffset();
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("LATEST")) {
			offsetForThisRound = kafkaConsumerClient.getLastestOffset();
		} else if (consumerConfig.startOffsetFrom.equalsIgnoreCase("RESTART")) {
			logger.info("Restarting from where the Offset is left for topic {}, for partition {}",currentTopic,currentPartition);
			offsetForThisRound = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
			if (offsetForThisRound == -1)
			{
				// if this is the first time this client tried to read - offset might be -1
				// [ TODO figure out all cases when this could happen]
				// try to get the Earliest offset and read from there - it may lead
				// to processing events that may have already be processed - but it is safer than
				// starting from the Latest offset in case not all events were processed before				
				offsetForThisRound = kafkaConsumerClient.getEarliestOffset();
				logger.info("offsetForThisRound is set to the EarliestOffset since currentOffset is -1; offsetForThisRound={} for partition {}s",
						offsetForThisRound,currentPartition);
				// also store this as the CurrentOffset to Kafka - to avoid the multiple cycles through
				// this logic in the case no events are coming to the topic for a long time and
				// we always get currentOffset as -1 from Kafka
				try {
					kafkaConsumerClient.saveOffsetInKafka( offsetForThisRound, ErrorMapping.NoError());
				} catch (Exception e) {
					logger.error("Failed to commit the offset in Kafka, exiting for partition {}: " ,currentPartition, e);
					throw new Exception("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
				}

			} else {
				logger.info("offsetForThisRound is set to the CurrentOffset: {} for partition {}", offsetForThisRound,currentPartition);
			}
		}
		long earliestOffset = kafkaConsumerClient.getEarliestOffset();
		// check for a corner case when the computed offset (either current or custom)
		// is less than the Earliest offset - which could happen if some messages were 
		// cleaned up from the topic/partition due to retention policy
		if (offsetForThisRound < earliestOffset){
			logger.warn("WARNING: computed offset (either current or custom) = {} is less than EarliestOffset = {}" +
					"; setting offsetForThisRound to the EarliestOffset for partition {}", offsetForThisRound, earliestOffset,currentPartition);
			offsetForThisRound = earliestOffset;
			try {
				kafkaConsumerClient.saveOffsetInKafka( offsetForThisRound, ErrorMapping.NoError());
			} catch (Exception e) {
				logger.error("Failed to commit the offset in Kafka, exiting for partition {} " ,currentPartition, e);
				throw new Exception("Failed to commit the offset in Kafka, exiting: " + e.getMessage(), e);
			}
		}
		logger.info("Resulting offsetForThisRound = {} for partition {}", offsetForThisRound,currentPartition);
	}

	public IndexerJobStatus call() {
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Started);
        while(!shutdownRequested){
        	try{
        		// check if there was a request to stop this thread - stop processing if so
                if (Thread.currentThread().isInterrupted()){
                    // preserve interruption state of the thread
                    Thread.currentThread().interrupt();
                    throw new InterruptedException(
                    	"Cought interrupted event in IndexerJob for partition=" + currentPartition + " - stopping");
                }
        		logger.info("******* Starting a new batch of events from Kafka for partition {} ...", currentPartition);
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
        		processBatch();
        		// sleep for configured time
        		// TODO improve sleep pattern
        		Thread.sleep(consumerConfig.consumerSleepBetweenFetchsMs * 1000);
        		logger.debug("Completed a round of indexing into ES for partition {}",currentPartition);
        	} catch (InterruptedException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
        		stopClients();
        	} catch(Exception e){
        		logger.error("Exception when starting a new round of kafka Indexer job, exiting: for partition {} " ,currentPartition,e);
        		// do not keep going if a severe error happened - stop and fix the issue manually,
        		// then restart the consumer again; It is better to monitor the job externally 
        		// via Zabbix or the likes - rather then keep failing [potentially] forever
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        	}       
        }
		logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);
		return indexerJobStatus;
	}
	
	public void processBatch() throws Exception {
		checkKafkaOffsets();
		long jobStartTime = 0l;
		if (consumerConfig.isPerfReportingEnabled)
			jobStartTime = System.currentTimeMillis();
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
				logger.error("Exception getting Kafka offsets - exiting for partition {} ",currentPartition, e);
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
		indexerJobStatus.setLastCommittedOffset(offsetForThisRound);
		
		fetchResponse = kafkaConsumerClient.getMessagesFromKafka(offsetForThisRound);
		if (consumerConfig.isPerfReportingEnabled) {
			long timeAfterKafaFetch = System.currentTimeMillis();
			logger.debug("Fetched the reponse from Kafka. Approx time taken is {} ms for partition {}",
					(timeAfterKafaFetch - jobStartTime),currentPartition);
		}
		if (fetchResponse.hasError()) {
			// Do things according to the error code
			// TODO figure out what job status we should use - PartialFailure?? and when to clean it up?
			handleError();
			return;
		}
		
		// TODO harden the byteBufferMessageSEt life-cycle - make local var
		byteBufferMsgSet = kafkaConsumerClient.fetchMessageSet(fetchResponse);
		if (consumerConfig.isPerfReportingEnabled) {
			long timeAfterKafkaFetch = System.currentTimeMillis();
			logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is {} ms for partition {}",
					(timeAfterKafkaFetch - jobStartTime) ,currentPartition);
		}
		if (byteBufferMsgSet.validBytes() <= 0) {
			logger.warn("No events were read from Kafka - finishing this round of reads from Kafka for partition {}",currentPartition);
			// TODO re-review this logic
			long latestOffset = kafkaConsumerClient.getLastestOffset();
			if (latestOffset != offsetForThisRound) {
				logger.warn("latestOffset [{}] is not the same as the current offsetForThisRound for this run [{}]" +
						" - committing latestOffset to Kafka for partition {}", latestOffset, offsetForThisRound,currentPartition);
				try {
					kafkaConsumerClient.saveOffsetInKafka(
						latestOffset, 
						fetchResponse.errorCode(consumerConfig.topic, currentPartition));
				} catch (Exception e) {
					// throw an exception as this will break reading messages in the next round
					logger.error("Failed to commit the offset in Kafka - exiting for partition {} ",currentPartition, e);
					throw e;
				}
			}
			return;
		}
		logger.debug("Starting to prepare for post to ElasticSearch for partition {}",currentPartition);
		nextOffsetToProcess = msgHandler.prepareForPostToElasticSearch(byteBufferMsgSet.iterator());

		if (consumerConfig.isPerfReportingEnabled) {
			long timeAtPrepareES = System.currentTimeMillis();
			logger.debug("Completed preparing for post to ElasticSearch. Approx time taken: {}ms for partition {}",
					(timeAtPrepareES - jobStartTime),currentPartition );
		}
		if (isDryRun) {
			logger.info("**** This is a dry run, NOT committing the offset in Kafka nor posting to ES for partition {}****",currentPartition);
			return;
		}
		try {
			logger.info("posting the messages to ElasticSearch for partition {}...",currentPartition);
			msgHandler.postToElasticSearch();
		} catch (ElasticsearchException e) {
			// TODO decide how to handle / fail in this case
			// for now - just continue and commit the offset, 
			// but be aware that ALL messages from this batch are NOT indexed into ES
			logger.error("Error posting messages to Elastic Search for offset {}-->{} in partition {} skipping them: ",offsetForThisRound,nextOffsetToProcess-1,currentPartition, e);
			FailedEventsLogger.logFailedEvent(offsetForThisRound, nextOffsetToProcess - 1, currentPartition, e.getDetailedMessage(), null);
			// do not re-init ES - as we do not know if this would help or not
			//this.reInitElasticSearch();
		}

		if (consumerConfig.isPerfReportingEnabled) {
			long timeAftEsPost = System.currentTimeMillis();
			logger.debug("Approx time to post of ElasticSearch: {} ms for partition {}",
					(timeAftEsPost - jobStartTime),currentPartition);
		}
		logger.info("Commiting offset: {} for partition {}", nextOffsetToProcess,currentPartition);
		// TODO optimize getting of the fetchResponse.errorCode - in some cases there is no error, 
		// so no need to call the API every time
		try {
			kafkaConsumerClient.saveOffsetInKafka(
					nextOffsetToProcess, fetchResponse.errorCode(
							consumerConfig.topic, currentPartition));
		} catch (Exception e) {
			logger.error("Failed to commit the Offset in Kafka after processing and posting to ES for partition {}: ",currentPartition, e);
			logger.info("Trying to reInitialize Kafka and commit the offset again for partition {}...",currentPartition);
			reInitKafka();
			try {
				logger.info("Attempting to commit the offset after reInitializing Kafka now..");
				kafkaConsumerClient.saveOffsetInKafka(
					nextOffsetToProcess, fetchResponse.errorCode(
						consumerConfig.topic,
						currentPartition));
			} catch (Exception e2) {
				logger.error("Failed to commit the Offset in Kafka even after reInitializing Kafka - exiting for partition {}: " ,currentPartition, e2);
				// there is no point in continuing  - as we will keep re-processing events
				// from the old offset. Throw an exception and exit;
				// manually fix KAfka/Zookeeper env and re-start from a 
				// desired , possibly custom, offset
				throw e2;
			}
		}

		if (consumerConfig.isPerfReportingEnabled) {
			long timeAtEndOfJob = System.currentTimeMillis();
			logger.info("*** This round of ConsumerJob took about {} ms for partition {} ",
					(timeAtEndOfJob - jobStartTime),currentPartition);
		}
		logger.info("*** Finished current round of ConsumerJob, processed messages with offsets [{}-{}] for partition {} ****",
				offsetForThisRound, nextOffsetToProcess,currentPartition);
		this.byteBufferMsgSet = null;
		this.fetchResponse = null;
	}

	public void handleError() throws Exception {
		// Do things according to the error code
		short errorCode = fetchResponse.errorCode(
				consumerConfig.topic, currentPartition);
		logger.error("Error fetching events from Kafka - handling it. Error code: {}  for partition {}"
				, errorCode,currentPartition);
		if (errorCode == ErrorMapping.BrokerNotAvailableCode()) {
			logger.error("BrokerNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client for partition {}",currentPartition);
			reInitKafka();
		} else if (errorCode == ErrorMapping.InvalidFetchSizeCode()) {
			logger.error("InvalidFetchSizeCode error happened when fetching message from Kafka. ReInitiating Kafka Client for partition {}",currentPartition);
			reInitKafka();
		} else if (errorCode == ErrorMapping.InvalidMessageCode()) {
			logger.error("InvalidMessageCode error happened when fetching message from Kafka, not handling it. Returning for partition {}",currentPartition);
		} else if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
			logger.error("LeaderNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client for partition {}",currentPartition);
			reInitKafka();
		} else if (errorCode == ErrorMapping.MessageSizeTooLargeCode()) {
			logger.error("MessageSizeTooLargeCode error happened when fetching message from Kafka, not handling it. Returning for partition {}",currentPartition);
		} else if (errorCode == ErrorMapping.NotLeaderForPartitionCode()) {
			logger.error("NotLeaderForPartitionCode error happened when fetching message from Kafka, not handling it. ReInitiating Kafka Client for partition {}",currentPartition);
			reInitKafka();
		} else if (errorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
			logger.error("OffsetMetadataTooLargeCode error happened when fetching message from Kafka, not handling it. Returning for partition {}",currentPartition);
		} else if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
			logger.error("OffsetOutOfRangeCode error happened when fetching message from Kafka for partition {}",currentPartition);
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
			logger.error("ReplicaNotAvailableCode error happened when fetching message from Kafka - re-init-ing Kafka... for partition {}",currentPartition);
			reInitKafka();
			return;

		} else if (errorCode == ErrorMapping.RequestTimedOutCode()) {
			logger.error("RequestTimedOutCode error happened when fetching message from Kafka - re-init-ing Kafka... for partition {}",currentPartition);
			reInitKafka();
			return;

		} else if (errorCode == ErrorMapping.StaleControllerEpochCode()) {
			logger.error("StaleControllerEpochCode error happened when fetching message from Kafka, not handling it. Returning for partition {}",currentPartition);
			return;

		} else if (errorCode == ErrorMapping.StaleLeaderEpochCode()) {
			logger.error("StaleLeaderEpochCode error happened when fetching message from Kafka, not handling it. Returning for partition {}",currentPartition);
			return;

		} else if (errorCode == ErrorMapping.UnknownCode()) {
			logger.error("UnknownCode error happened when fetching message from Kafka - re-init-ing Kafka... for partition {}",currentPartition);
			reInitKafka();
			return;

		} else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
			logger.error("UnknownTopicOrPartitionCode error happened when fetching message from Kafka - re-init-ing Kafka...for partition {}",currentPartition);
			reInitKafka();
			return;

		}

	}
	public void stopClients() {
		logger.info("About to stop ES client ");
		if (esClient != null)
			esClient.close();
		logger.info("About to stop Kafka client ");
		if (kafkaConsumerClient != null)
			kafkaConsumerClient.close();
		logger.info("Stopped Kafka client");
	}

}
