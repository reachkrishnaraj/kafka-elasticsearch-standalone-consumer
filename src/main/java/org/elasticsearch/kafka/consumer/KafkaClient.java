package org.elasticsearch.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaClient {

	
	private static final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
	// TODO externalize this property
	private final int FIND_KAFKA_LEADER_TIMEOUT = 10000;
	CuratorFramework curator;
	SimpleConsumer simpleConsumer;
	String zooKeeper;
	String incomingBrokerHost;
	int inComingBrokerPort;
	String inComingbrokerURL;
	String kafkaGroupId;
	String topic;
	int partition;
	List<String> m_replicaBrokers;
	String leadBrokerHost;
	int leadBrokerPort;
	String leadBrokerURL;
	ConsumerConfig consumerConfig;

	
	public KafkaClient(ConsumerConfig a_config, String a_zooKeeper, String a_brokerHost, 
			int a_port, int a_partition, String a_kafkaGroupId, String a_topic) throws Exception {
		logger.info("Instantiating KafkaClient");
		this.consumerConfig = a_config;
		
		this.zooKeeper = a_zooKeeper;
		this.topic = a_topic;
		this.incomingBrokerHost = a_brokerHost;
		this.inComingBrokerPort = a_port;
		this.inComingbrokerURL = this.incomingBrokerHost + ":" + this.inComingBrokerPort;
		this.kafkaGroupId = a_kafkaGroupId;
		this.partition = a_partition;
		logger.info("### KafkaClient Config: ###");
		logger.info("zooKeeper: {}", this.zooKeeper);
		logger.info("topic: {}", this.topic);
		logger.info("incomingBrokerHost: {}", this.incomingBrokerHost);
		logger.info("inComingBrokerPort: {}", this.inComingBrokerPort);
		logger.info("inComingbrokerURL: {}", this.inComingbrokerURL);
		logger.info("kafkaGroupId: {}", this.kafkaGroupId);
		logger.info("partition: {}", this.partition);
		m_replicaBrokers = new ArrayList<String>();
		logger.info("Starting to connect to Zookeeper");
		connectToZooKeeper(this.zooKeeper);
		logger.info("Starting to find the Kafka Lead Broker");
		findLeader();
		logger.info("Starting to initiate Kafka Consumer");
		initConsumer();
		
	}
	
	void connectToZooKeeper() throws Exception {
		connectToZooKeeper(this.zooKeeper);
	}
	
	
	void connectToZooKeeper(String zooKeeper) throws Exception {
		try {
			curator = CuratorFrameworkFactory.newClient(zooKeeper, 1000, 15000,
					new RetryNTimes(5, 2000));
			curator.start();
			logger.info("Connection to Kafka Zookeeper successfull");
		} catch (Exception e) {
			logger.error("Failed to connect to Zookeer: " + e.getMessage());	
			throw e;
		}
	}

	public void initConsumer() throws Exception{
		try{
			this.simpleConsumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort, 10000, 1024 * 1024 * 10, kafkaGroupId);
			logger.info("Succesfully initialized Kafka Consumer");
		}
		catch(Exception e){
			logger.error("Failed to initialize Kafka Consumer: " + e.getMessage());
			throw e;
		}
	}

	public short saveOffsetInKafka(long offset, short errorCode) throws Exception{
		logger.debug("Starting to save the Offset value to Kafka: offset={}, errorCode={}",
				offset, errorCode);
		short versionID = 0;
		int correlationId = 0;
		try{
			TopicAndPartition tp = new TopicAndPartition(this.topic, this.partition);
			OffsetAndMetadata offsetMetaAndErr = new OffsetAndMetadata(
					offset, OffsetAndMetadata.NoMetadata(), errorCode);
			Map<TopicAndPartition, OffsetAndMetadata> mapForCommitOffset = new HashMap<>();
			mapForCommitOffset.put(tp, offsetMetaAndErr);
			kafka.javaapi.OffsetCommitRequest offsetCommitReq = new kafka.javaapi.OffsetCommitRequest(
					kafkaGroupId, mapForCommitOffset, correlationId, kafkaGroupId, versionID);
			OffsetCommitResponse offsetCommitResp = this.simpleConsumer.commitOffsets(offsetCommitReq);
			logger.debug("Completed OffsetSet commit. OffsetCommitResponse ErrorCode = {} ", offsetCommitResp.errors().get(tp)+ " Returning to the caller.");
			return (Short) offsetCommitResp.errors().get(tp);
		}
		catch(Exception e){
			logger.error("Error when commiting Offset to Kafka: " + e.getMessage(), e);
			throw e;
		}
	}
		
	private PartitionMetadata findLeader() throws Exception {
		logger.info("Starting to find the leader broker for Kafka");
		PartitionMetadata returnMetaData = null;
		SimpleConsumer leadFindConsumer = null;
		try {
			leadFindConsumer = new SimpleConsumer(incomingBrokerHost, inComingBrokerPort, FIND_KAFKA_LEADER_TIMEOUT,
					consumerConfig.bulkSize, "leaderLookup");
			List<String> topics = new ArrayList<String>();
			topics.add(this.topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse resp = leadFindConsumer.send(req);

			List<TopicMetadata> metaData = resp.topicsMetadata();
			for (TopicMetadata item : metaData) {
				for (PartitionMetadata part : item.partitionsMetadata()) {
					if (part.partitionId() == this.partition) {
						//System.out.println("ITS TRUE");
						returnMetaData = part;
					break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error communicating with Broker [{}] to find Leader for topic=[{}], partition=[{}" + partition
					+ "] Error: " + e.getMessage(), incomingBrokerHost,topic, partition, e.getMessage());
			throw e;
		} finally {
			if (leadFindConsumer != null)
				leadFindConsumer.close();
			logger.info("closed the connection");
			
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		
		this.leadBrokerHost = returnMetaData.leader().host();
		this.leadBrokerPort = returnMetaData.leader().port();
		this.leadBrokerURL = this.leadBrokerHost + ":" + this.leadBrokerPort;
		logger.info("Computed leadBrokerURL: {} , returning ", this.leadBrokerURL);
		return returnMetaData;
	}

	
	public PartitionMetadata findNewLeader() throws Exception {
		logger.info("Starting to findNewLeader for kafka ...");
		for (int i = 0; i < 3; i++) {
			logger.info("Attempt #{}", i);
			boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
            	logger.info("MetaData is Empty, going to sleep");
            	goToSleep = true;
            } else if (metadata.leader() == null) {
            	logger.info("MetaData leader is Empty, going to sleep");
            	goToSleep = true;
            } else if (this.leadBrokerHost.equalsIgnoreCase(metadata.leader().host()) && i <= 1) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
            	this.leadBrokerHost = metadata.leader().host();
        		this.leadBrokerPort = metadata.leader().port();
        		this.leadBrokerURL = this.leadBrokerHost + ":" + this.leadBrokerPort;
        		logger.info("Found and Computed leadBrokerURL: {}", leadBrokerURL);
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
		logger.error("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
	
	
	public long getLastestOffset() throws Exception {
		logger.debug("Getting LastestOffset for topic={}, partition={}, kafkaGroupId={}", 
				topic, partition, kafkaGroupId);
		long latestOffset = getOffset(topic, partition, OffsetRequest.LatestTime(), kafkaGroupId);
		logger.debug("LatestOffset={}", latestOffset);
		return latestOffset;
		
	}
	
	public long getEarliestOffset() throws Exception {
		logger.debug("Getting EarliestOffset for topic={}, partition={}, kafkaGroupId={}", 
				topic, partition, kafkaGroupId);
		long earliestOffset = this.getOffset(topic, partition, OffsetRequest.EarliestTime(), kafkaGroupId);
		logger.debug("earliestOffset={}" + earliestOffset);
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
				logger.error("Error fetching offsets from Kafka. Reason: " + response.errorCode(topic, partition));
				throw new Exception("Error fetching offsets from Kafka. Reason: " + response.errorCode(topic, partition));
			}
			long[] offsets = response.offsets(topic, partition);
			return offsets[0];

		}
	catch(Exception e){
		logger.error("Exception when trying to get the Offset. Throwing the exception. Error is:" + e.getMessage());
		throw e;
	}
	}
	
	public long fetchCurrentOffsetFromKafka() throws Exception{
		//logger.info("Starting to fetchCurrentOffsetFromKafka");
		// versionID = 0 - fetches offsets from Zookeeper
		// versionID = 1 and above - from KAfka (version 0.8.2.1 and above)
		short versionID = 0;
		int correlationId = 0;
		try{
			List<TopicAndPartition> topicPartitionList = new ArrayList<TopicAndPartition>(); 
			TopicAndPartition myTopicAndPartition = new TopicAndPartition(topic, partition);
			topicPartitionList.add(myTopicAndPartition);	
			OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(
					kafkaGroupId, topicPartitionList, versionID, correlationId, kafkaGroupId);
			OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchReq);
			long currentOffset = offsetFetchResponse.offsets().get(myTopicAndPartition).offset();
			//logger.info("Fetched Kafka's currentOffset = " + currentOffset);
			return currentOffset;
		}
		catch(Exception e){
			logger.error("Error when fetching current offset from kafka: " + e.getMessage());
			throw e;
		}
	}


	FetchResponse getMessagesFromKafka(long offset, int maxSizeBytes) throws Exception {
		logger.debug("Starting getMessagesFromKafka() ...");
		try{
			FetchRequest req = new FetchRequestBuilder()
				.clientId(kafkaGroupId)
				.addFetch(topic, partition, offset, maxSizeBytes)
				.build();
			FetchResponse fetchResponse = simpleConsumer.fetch(req);
			return fetchResponse;
		}
		catch(Exception e){
			logger.error("Exception fetching messages from Kafka: " + e.getMessage(), e);
			throw e;
		}
	}
	
	ByteBufferMessageSet fetchMessageSet(FetchResponse fetchReponse){
		return fetchReponse.messageSet(topic, partition);
	}
	
	public void close() {
		curator.close();
		logger.info("Curator/Zookeeper connection closed");
	}
	
}
