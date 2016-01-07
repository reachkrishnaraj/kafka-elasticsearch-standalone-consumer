package org.elasticsearch.kafka.indexer;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClient {

	
	private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
	private CuratorFramework curator;
	private SimpleConsumer simpleConsumer;
	private String kafkaClientId;
	private String topic;
	private final int partition;
	private String leaderBrokerHost;
	private int leaderBrokerPort;
	private String leaderBrokerURL;
	private final ConsumerConfig consumerConfig;
	private String[] kafkaBrokersArray;

	
	public KafkaClient(final ConsumerConfig config, String kafkaClientId, int partition) throws Exception {
		logger.info("Instantiating KafkaClient");
		this.consumerConfig = config;
		
		this.topic = config.topic;
		this.kafkaClientId = kafkaClientId;
		this.partition = partition;
		kafkaBrokersArray = config.kafkaBrokersList.trim().split(",");
		logger.info("### KafkaClient Config: ###");
		logger.info("kafkaZookeeperList: {}", config.kafkaZookeeperList);
		logger.info("kafkaBrokersList: {}", config.kafkaBrokersList);
		logger.info("kafkaClientId: {}", kafkaClientId);
		logger.info("topic: {}", topic);
		logger.info("partition: {}", partition);
		connectToZooKeeper();
		findLeader();
		initConsumer();
		
	}
			
	public void connectToZooKeeper() throws Exception {
		try {
			curator = CuratorFrameworkFactory.newClient(consumerConfig.kafkaZookeeperList, 
					consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs,
					new RetryNTimes(consumerConfig.zkCuratorRetryTimes, consumerConfig.zkCuratorRetryDelayMs));
			curator.start();
			logger.info("Connected to Kafka Zookeeper successfully");
		} catch (Exception e) {
			logger.error("Failed to connect to Zookeer: " + e.getMessage());	
			throw e;
		}
	}

	public void initConsumer() throws Exception{
		try{
			this.simpleConsumer = new SimpleConsumer(
					leaderBrokerHost, leaderBrokerPort, 
					consumerConfig.kafkaSimpleConsumerSocketTimeoutMs, 
					consumerConfig.kafkaSimpleConsumerBufferSizeBytes, 
					kafkaClientId);
			logger.info("Initialized Kafka Consumer successfully for partition {}",partition);
		}
		catch(Exception e){
			logger.error("Failed to initialize Kafka Consumer: " + e.getMessage());
			throw e;
		}
	}

	public short saveOffsetInKafka(long offset, short errorCode) throws Exception{
		logger.debug("Starting to save the Offset value to Kafka: offset={}, errorCode={} for partition {}",
				offset, errorCode,partition);
		short versionID = 0;
		int correlationId = 0;
		try{
			TopicAndPartition tp = new TopicAndPartition(topic, partition);
			OffsetAndMetadata offsetMetaAndErr = new OffsetAndMetadata(
					offset, OffsetAndMetadata.NoMetadata(), errorCode);
			Map<TopicAndPartition, OffsetAndMetadata> mapForCommitOffset = new HashMap<>();
			mapForCommitOffset.put(tp, offsetMetaAndErr);
			kafka.javaapi.OffsetCommitRequest offsetCommitReq = new kafka.javaapi.OffsetCommitRequest(
					kafkaClientId, mapForCommitOffset, correlationId, kafkaClientId, versionID);
			OffsetCommitResponse offsetCommitResp = simpleConsumer.commitOffsets(offsetCommitReq);
			logger.debug("Completed OffsetSet commit for partition {}. OffsetCommitResponse ErrorCode = {} Returning to caller ", partition,offsetCommitResp.errors().get(tp));
			return (Short) offsetCommitResp.errors().get(tp);
		}
		catch(Exception e){
			logger.error("Error when commiting Offset to Kafka: " + e.getMessage(), e);
			throw e;
		}
	}
		
	public PartitionMetadata findLeader() throws Exception {
		logger.info("Looking for Kafka leader broker for partition {}...", partition);
		PartitionMetadata leaderPartitionMetaData = null;
		// try to find leader META info, trying each broker until the leader is found - 
		// in case some of the leaders are down
		for (int i=0; i<kafkaBrokersArray.length; i++) {
			String brokerStr = kafkaBrokersArray[i];
			String[] brokerStrTokens = brokerStr.split(":");
			if (brokerStrTokens.length < 2) {
				logger.error(
					"Failed to find Kafka leader broker - wrong config, not enough tokens: brokerStr={}", 
					brokerStr);
				throw new Exception("Failed to find Kafka leader broker - wrong config, not enough tokens: brokerStr=" + 
					brokerStr);
			}
			leaderPartitionMetaData = findLeaderWithBroker(brokerStrTokens[0], brokerStrTokens[1]);
			// we found the leader - no need to query the rest of the brokers, get out of the loop
			if (leaderPartitionMetaData != null)
				break;
		}
		if (leaderPartitionMetaData == null || leaderPartitionMetaData.leader() == null) {
			logger.error("Failed to find leader for topic=[{}], partition=[{}], kafka brokers list: [{}]: PartitionMetadata is null",
					topic, partition, consumerConfig.kafkaBrokersList);
			throw new Exception("Failed to find leader for topic=[" + topic + 
					"], partition=[" + partition + 
					", kafka brokers list: [" + consumerConfig.kafkaBrokersList +
					"]: currentPartitionMetadata is null");
			
		}		
		leaderBrokerHost = leaderPartitionMetaData.leader().host();
		leaderBrokerPort = leaderPartitionMetaData.leader().port();
		leaderBrokerURL = leaderBrokerHost + ":" + leaderBrokerPort;
		logger.info("Found leader: leaderBrokerURL={}", leaderBrokerURL);
		return leaderPartitionMetaData;
	}
	
	private PartitionMetadata findLeaderWithBroker(
			String kafkaBrokerHost, String kafkaBrokerPortStr) {
		logger.info("Looking for leader for partition {} using Kafka Broker={}:{}, topic={}",
				partition, kafkaBrokerHost, kafkaBrokerPortStr, topic);
		PartitionMetadata leaderPartitionMetadata = null;
		SimpleConsumer leadFindConsumer = null;
		try {
			int kafkaBrokerPort = Integer.parseInt(kafkaBrokerPortStr);
			leadFindConsumer = new SimpleConsumer(
					kafkaBrokerHost, kafkaBrokerPort, 
					consumerConfig.kafkaSimpleConsumerSocketTimeoutMs, 
					consumerConfig.kafkaSimpleConsumerBufferSizeBytes, 
					"leaderLookup");
			List<String> topics = new ArrayList<String>();
			topics.add(this.topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse resp = leadFindConsumer.send(req);

			List<TopicMetadata> metaData = resp.topicsMetadata();
			for (TopicMetadata item : metaData) {
				for (PartitionMetadata part : item.partitionsMetadata()) {
					if (part.partitionId() == partition) {
						leaderPartitionMetadata = part;
						logger.info("Found leader for partition {} using Kafka Broker={}:{}, topic={}; leader broker URL: {}:{}",
								partition, kafkaBrokerHost, kafkaBrokerPortStr, topic,
								leaderPartitionMetadata.leader().host(), leaderPartitionMetadata.leader().port());
						break;
					}
				}
				// we found the leader - get out of this loop as well
				if (leaderPartitionMetadata != null)
					break;
			}
		} catch (Exception e) {
			logger.warn("Failed to find leader for partition {} using Kafka Broker={} , topic={}; Error: {}",
					partition, kafkaBrokerHost, topic, e.getMessage());
		} finally {
			if (leadFindConsumer != null){
				leadFindConsumer.close();
				logger.debug("Closed the leadFindConsumer connection");
			}
		}
		return leaderPartitionMetadata;	
	}

	public long getLastestOffset() throws Exception {
		logger.debug("Getting LastestOffset for topic={}, partition={}, kafkaGroupId={}",
				topic, partition, kafkaClientId);
		long latestOffset = getOffset(topic, partition, OffsetRequest.LatestTime(), kafkaClientId);
		logger.debug("LatestOffset={}  for partition {}", latestOffset ,partition);
		return latestOffset;		
	}
	
	public long getEarliestOffset() throws Exception {
		logger.debug("Getting EarliestOffset for topic={}, partition={}, kafkaGroupId={}",
				topic, partition, kafkaClientId);
		long earliestOffset = this.getOffset(topic, partition, OffsetRequest.EarliestTime(), kafkaClientId);
		logger.debug("earliestOffset={} for partition {}", earliestOffset,partition);
		return earliestOffset;
	}
	
	private long getOffset(String topic, int partition, long whichTime, String clientName) throws Exception {
		try{
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
					partition);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
					whichTime, 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
					requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
					clientName);
			OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

			if (response.hasError()) {
				logger.error("Error fetching offsets from Kafka. Reason: {} for partition {}" , response.errorCode(topic, partition),partition);
				throw new Exception("Error fetching offsets from Kafka. Reason: " + response.errorCode(topic, partition) +"for partition "+partition);
			}
			long[] offsets = response.offsets(topic, partition);
			return offsets[0];
		}
		catch(Exception e){
			logger.error("Exception when trying to get the Offset. Throwing the exception for partition {}" ,partition,e);
			throw e;
		}
	}
	
	public long fetchCurrentOffsetFromKafka() throws Exception{
		short versionID = 0;
		int correlationId = 0;
		try{
			List<TopicAndPartition> topicPartitionList = new ArrayList<TopicAndPartition>(); 
			TopicAndPartition myTopicAndPartition = new TopicAndPartition(topic, partition);
			topicPartitionList.add(myTopicAndPartition);	
			OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(
					kafkaClientId, topicPartitionList, versionID, correlationId, kafkaClientId);
			OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchReq);
			long currentOffset = offsetFetchResponse.offsets().get(myTopicAndPartition).offset();
			//logger.info("Fetched Kafka's currentOffset = " + currentOffset);
			return currentOffset;
		}
		catch(Exception e){
			logger.error("Error when fetching current offset from kafka: for partition {}" ,partition,e);
			throw e;
		}
	}


	public FetchResponse getMessagesFromKafka(long offset) throws Exception {
		logger.debug("Starting getMessagesFromKafka() ...");
		try{
			FetchRequest req = new FetchRequestBuilder()
				.clientId(kafkaClientId)
				.addFetch(topic, partition, offset, consumerConfig.kafkaFetchSizeMinBytes)
				.build();
			FetchResponse fetchResponse = simpleConsumer.fetch(req);
			return fetchResponse;
		}
		catch(Exception e){
			logger.error("Exception fetching messages from Kafka for partition {}" ,partition,e);
			throw e;
		}
	}
		
	public void close() {
		curator.close();
		logger.info("Curator/Zookeeper connection closed");
	}
	
}
