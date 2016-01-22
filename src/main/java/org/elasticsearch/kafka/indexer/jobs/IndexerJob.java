package org.elasticsearch.kafka.indexer.jobs;

import java.util.concurrent.Callable;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.IndexerESException;
import org.elasticsearch.kafka.indexer.KafkaClient;
import org.elasticsearch.kafka.indexer.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;

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

	private int kafkaReinitSleepTimeMs;
	private int numberOfReinitAttempts;
	private int esIndexingRetrySleepTimeMs;
	private int numberOfEsIndexingRetryAttempts;
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
		kafkaReinitSleepTimeMs = config.getKafkaReinitSleepTimeMs();
		numberOfReinitAttempts = config.getNumberOfReinitAttempts();
		esIndexingRetrySleepTimeMs = config.getEsIndexingRetrySleepTimeMs();
		numberOfEsIndexingRetryAttempts = config.getNumberOfEsIndexingRetryAttempts();
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
		for (int i = 0; i < numberOfReinitAttempts; i++) {
			try {
				logger.info("Re-initializing Kafka for partition {}, try # {}",
						currentPartition, i);
				kafkaConsumerClient.close();
				logger.info(
						"Kafka client closed for partition {}. Will sleep for {} ms to allow kafka to stabilize",
						currentPartition, kafkaReinitSleepTimeMs);
				Thread.sleep(kafkaReinitSleepTimeMs);
				logger.info("Connecting to zookeeper again for partition {}",
						currentPartition);
				kafkaConsumerClient.connectToZooKeeper();
				kafkaConsumerClient.findLeader();
				kafkaConsumerClient.initConsumer();
				logger.info(".. trying to get offsets info for partition {} ... ", currentPartition);
				this.checkKafkaOffsets();
				logger.info("Kafka Reintialization  for partition {} finished OK",
						currentPartition);
				return;
			} catch (Exception e) {
				if (i < numberOfReinitAttempts) {
					logger.info("Re-initializing Kafka for partition {}, try # {} - still failing with Exception",
						currentPartition, i);		
				} else {
					// if we got here - we failed to re-init Kafka after numberOfTries attempts - throw an exception out
					logger.info("Kafka Re-initialization failed for partition {} after {} attempts - throwing exception: "
							+ e.getMessage(), currentPartition, numberOfReinitAttempts);
					throw e;
				}
			}
		}		
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

	public void checkKafkaOffsets() throws Exception {
		try {
			long currentOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
			long earliestOffset = kafkaConsumerClient.getEarliestOffset();
			long latestOffset = kafkaConsumerClient.getLastestOffset();
			logger.info("Kafka offsets: currentOffset={}; earliestOffset={}; latestOffset={} for partition {}",
					currentOffset, earliestOffset, latestOffset,currentPartition);
		} catch (Exception e) {
			logger.error("Exception from checkKafkaOffsets(): for partition {}" ,currentPartition, e);
			throw e;
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
				logger.info("offsetForThisRound is set to the EarliestOffset since currentOffset is -1; offsetForThisRound={} for partition {}",
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
		logger.info("EarliestOffset for partition {} is {}", currentPartition, earliestOffset);
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
        		logger.debug("******* Starting a new batch of events from Kafka for partition {} ...", currentPartition);
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
        		processBatch();
        		// sleep for configured time
        		// TODO improve sleep pattern
        		Thread.sleep(consumerConfig.consumerSleepBetweenFetchsMs * 1000);
        		logger.debug("Completed a round of indexing into ES for partition {}",currentPartition);
        	} catch (IndexerESException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        		break;
        	} catch (InterruptedException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
        		stopClients();
        		break;
        	} catch (Exception e){
        		logger.error("Exception when starting a new round of kafka Indexer job for partition {} - will try to re-init Kafka " ,
        				currentPartition, e);
        		// try to re-init Kafka connection first - in case the leader for this partition
        		// has changed due to a Kafka node restart and/or leader re-election
        		try {
        			this.reInitKafka();
        		} catch (Exception e2) {
        			// we still failed - do not keep going anymore - stop and fix the issue manually,
            		// then restart the consumer again; It is better to monitor the job externally 
            		// via Zabbix or the likes - rather then keep failing [potentially] forever
            		logger.error("Exception when starting a new round of kafka Indexer job, partition {}, exiting: "
            				+ e2.getMessage(), currentPartition);
            		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
            		stopClients();  
            		break;
        		}
        	}       
        }
		logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);
		return indexerJobStatus;
	}
	
	
	
	public void processBatch() throws Exception {
		//checkKafkaOffsets();
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
				logger.error("Exception getting Kafka offsets for partition {}, will try to re-init Kafka ",
						currentPartition, e);
				reInitKafka();
				// if re-initialization was successful - return and let the next job run try to
				// pickup from where it was left of before
				// if it failed - an exception will be thrown out of the reInitKafka()
				return;
			}
			// mark this as not first time startup anymore - since we already saved correct offset
			// to Kafka, and to avoid going through the logic of figuring out the initial offset
			// every round if it so happens that there were no events from Kafka for a long time
			isStartingFirstTime = false;
			nextOffsetToProcess = offsetForThisRound;
		}
		indexerJobStatus.setLastCommittedOffset(offsetForThisRound);
		
		try{
			fetchResponse = kafkaConsumerClient.getMessagesFromKafka(offsetForThisRound);
		} catch (Exception e){
			// try re-process this batch
			reInitKafka();
			return;
		}
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
		byteBufferMsgSet = fetchResponse.messageSet(currentTopic, currentPartition);
		if (consumerConfig.isPerfReportingEnabled) {
			long timeAfterKafkaFetch = System.currentTimeMillis();
			logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is {} ms for partition {}",
					(timeAfterKafkaFetch - jobStartTime) ,currentPartition);
		}
		if (byteBufferMsgSet.validBytes() <= 0) {
			logger.debug("No events were read from Kafka - finishing this round of reads from Kafka for partition {}",currentPartition);
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
			this.indexIntoESWithRetries();
		} catch (IndexerESException e) {
			// re-process batch
			return;
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
			try {
				reInitKafka();
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

	private void reInitElasticSearch() throws InterruptedException, IndexerESException {
		for (int i=1; i<=numberOfEsIndexingRetryAttempts; i++ ){
			Thread.sleep(esIndexingRetrySleepTimeMs);
			logger.warn("Retrying connect to ES and re-process batch, partition {}, try# {}", 
					currentPartition, i);
			try {
				this.initElasticSearch();
				// we succeeded - get out of the loop
				break;
			} catch (Exception e2) {
				if (i<numberOfEsIndexingRetryAttempts){
					// do not fail yet - will re-try again
					indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Hanging);
					logger.warn("Retrying connect to ES and re-process batch, partition {}, try# {} - failed again", 
							currentPartition, i);						
				} else {
					//we've exhausted the number of retries - throw a IndexerESException to stop the IndexerJob thread
					logger.error("Retrying connect to ES and re-process batch after connection failure, partition {}, "
							+ "try# {} - failed after the last retry; Will keep retrying, ", 
							currentPartition, i);						
					
					indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Hanging);
					throw new IndexerESException("Indexing into ES failed due to connectivity issue to ES, partition: " +
						currentPartition);
				}
			}
		}
	}

	
	private void indexIntoESWithRetries() throws IndexerESException, Exception {
		try {
			logger.info("posting the messages to ElasticSearch for partition {}...",currentPartition);
			msgHandler.postToElasticSearch();
		} catch (NoNodeAvailableException e) {
			// ES cluster is unreachable or down. Re-try up to the configured number of times
			// if fails even after then - shutdown the current IndexerJob
			logger.error("Error posting messages to Elastic Search for offset {}-->{} " + 
					" in partition {}:  NoNodeAvailableException - ES cluster is unreachable, will retry to connect after sleeping for {}ms", 
					offsetForThisRound, nextOffsetToProcess-1, esIndexingRetrySleepTimeMs, currentPartition, e);
			
			reInitElasticSearch();
			//throws Exception to re-process current batch
			throw new IndexerESException();
			
		} catch (ElasticsearchException e) {
			// we are assuming that other exceptions are data-specific
			// -  continue and commit the offset, 
			// but be aware that ALL messages from this batch are NOT indexed into ES
			logger.error("Error posting messages to Elastic Search for offset {}-->{} in partition {} skipping them: ",
					offsetForThisRound, nextOffsetToProcess-1, currentPartition, e);
			FailedEventsLogger.logFailedEvent(offsetForThisRound, nextOffsetToProcess - 1, currentPartition, e.getDetailedMessage(), null);
		}
	
	}
	
	
	public void handleError() throws Exception {
		// Do things according to the error code
		short errorCode = fetchResponse.errorCode(
				consumerConfig.topic, currentPartition);
		logger.error("Error fetching events from Kafka - handling it. Error code: {}  for partition {}"
				,errorCode, currentPartition);
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
			logger.error("OffsetOutOfRangeCode error fetching messages for partition={}, offsetForThisRound={}",
					currentPartition, offsetForThisRound);
			long earliestOffset = kafkaConsumerClient.getEarliestOffset();
			// The most likely reason for this error is that the consumer is trying to read events from an offset
			// that has already expired from the Kafka topic due to retention period;
			// In that case the only course of action is to start processing events from the EARLIEST available offset
			logger.info("OffsetOutOfRangeCode error: setting offset for partition {} to the EARLIEST possible offset: {}", 
					currentPartition, earliestOffset);
			nextOffsetToProcess = earliestOffset;
			try {
				kafkaConsumerClient.saveOffsetInKafka(earliestOffset, errorCode);
			} catch (Exception e) {
				// throw an exception as this will break reading messages in the next round
				// TODO verify that the IndexerJob is stopped cleanly in this case
				logger.error("Failed to commit offset in Kafka after OffsetOutOfRangeCode - exiting for partition {} ", currentPartition, e);
				throw e;
			}
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
		logger.info("About to stop ES client for topic {}, partition {}", 
				currentTopic, currentPartition);
		if (esClient != null)
			esClient.close();
		logger.info("About to stop Kafka client for topic {}, partition {}", 
				currentTopic, currentPartition);
		if (kafkaConsumerClient != null)
			kafkaConsumerClient.close();
		logger.info("Stopped Kafka and ES clients for topic {}, partition {}", 
				currentTopic, currentPartition);
	}
	
	public IndexerJobStatus getIndexerJobStatus() {
		return indexerJobStatus;
	}

}
