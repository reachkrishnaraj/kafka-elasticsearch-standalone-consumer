package org.elasticsearch.kafka.consumer;

import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConfig {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);
	Properties prop = new Properties();
	InputStream input = null;
	// Logger object cannot be initialized since the logProperty file for the
	// instance would be known only after config is read
	// Logger logger = ConsumerLogger.initLogger(this.getClass());
	private final int BULK_MSG_SIZE = 10 * 1024 * 1024 * 3;
	private final int BULK_MSG_TIMEOUT = 10000;
	private final String BULK_MSG_TIMEOUT_STRING = "10ms";

	// Kafka ZooKeeper's IP Address/HostName without port
	public final String zookeeper;
	// Full class path and name for the concrete message handler class 
	public final String messageHandlerClass;
	// Full class name of a custom IndexHandler implementation class
	public final String indexHandlerClass;
	// Kafka Broker's IP Address/HostName
	public final String brokerHost;
	// Kafka's Broker List for producer
	public final String brokerListForProducer;
	// Kafka Broker's Port number
	public final int brokerPort;
	// Kafka Topic from which the message has to be processed
	public final String topic;
	// Partition in the Kafka Topic from which the message has to be processed
	public final short partition;
	// Option from where the message fetching should happen in Kafka
	// Values can be: CUSTOM/OLDEST/LATEST/RESTART.
	// If 'CUSTOM' is set, then 'startOffset' has to be set an int value
	public String startOffsetFrom;
	// int value of the offset from where the message processing should happen
	public final int startOffset;
	// Name of the Kafka Consumer Group
	public final String consumerGroupName;
	public final String statsdPrefix;
	public final String statsdHost;
	public final int statsdPort;
	// Preferred Size of message to be fetched from Kafka in 1 Fetch call to kafka
	public final int bulkSize;
	// Timeout when fetching message from Kafka
	public final TimeValue bulkTimeout;
	// Preferred Message Encoding to process the message before posting it to ElasticSearch
	public final String messageEncoding;
	// TBD
	public final boolean isGuranteedEsPostMode;
	// Name of the ElasticSearch Cluster
	public final String esClusterName;
	// Name of the ElasticSearch Host Port List
	public final String esHostPortList;
	// Hostname/ipAddress of ElasticSearch
	public final String esHost;
	// Port number of ElasticSearch
	public final int esPort;
	// IndexName in ElasticSearch to which the processed Message has to be posted
	public final String esIndex;
	// IndexType in ElasticSearch to which the processed Message has to be posted
	public final String esIndexType;
	// Percentage of message failure tolerance
	public final int esMsgFailureTolerancePercent;

	// Log property file for the consumer instance
	public final String logPropertyFile;

	// determines whether the consumer will post to ElasticSearch or not
	// If set to true, the consumer will read from Kafka, transform it and wont
	// post of ElasticSearch
	public final String isDryRun;

	// Wait time in seconds between consumer job rounds
	public final int consumerSleepTime;

	public String getStartOffsetFrom() {
		return startOffsetFrom;
	}

	public void setStartOffsetFrom(String startOffsetFrom) {
		this.startOffsetFrom = startOffsetFrom;
	}

	public ConsumerConfig(String configFile) throws Exception {
		try {
			logger.info("configFile : " + configFile);
			input = this.getClass().getClassLoader()
					.getResourceAsStream(configFile);
		} catch (Exception e) {
			// logger.fatal("Error reading/loading ConfigFile. Throwing the error. Error Message::"
			// + e.getMessage());
			logger.error("Error reading/loading configFile: " + e.getMessage(), e);
			e.printStackTrace();
			throw e;
		}

		if (input == null) {
			logger.info("Error loading config file - inputStream is null, exiting");
			throw new Exception("Error loading config file - inputStream is null");
		}

		// load the properties file
		prop.load(input);
		zookeeper = (String) prop.getProperty("zookeeper", "localhost");
		messageHandlerClass = prop.getProperty("messageHandlerClass",
						"org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler");
		indexHandlerClass = prop.getProperty("indexHandlerClass",
						"org.elasticsearch.kafka.consumer.BasicIndexHandler");
		brokerHost = prop.getProperty("brokerHost", "localhost");
		brokerPort = Integer.parseInt(prop.getProperty("brokerPort", "9092"));
		brokerListForProducer = prop.getProperty("brokerListForProducer",
				brokerHost + ":" + brokerPort);
		topic = prop.getProperty("topic", "");
		partition = Short.parseShort(prop.getProperty("partition", "0"));
		startOffsetFrom = prop.getProperty("startOffsetFrom", "");
		startOffset = Integer.parseInt(prop.getProperty("startOffset", ""));
		consumerGroupName = prop.getProperty("consumerGroupName",
				"ESKafkaConsumerClient");
		statsdPrefix = prop.getProperty("statsdPrefix", "");
		statsdHost = prop.getProperty("statsdHost", "");
		statsdPort = Integer.parseInt(prop.getProperty("statsdPort", "0"));
		bulkSize = Integer.parseInt(prop.getProperty("bulkSize",
				String.valueOf(BULK_MSG_SIZE)));
		bulkTimeout = TimeValue.parseTimeValue(
				(prop.getProperty("bulkTimeout", BULK_MSG_TIMEOUT_STRING)),
				TimeValue.timeValueMillis(BULK_MSG_TIMEOUT));
		messageEncoding = prop.getProperty("messageEncoding", "UTF-8");
		isGuranteedEsPostMode = Boolean.getBoolean(prop.getProperty(
				"isGuranteedEsPostMode", "false"));

		esClusterName = prop.getProperty("esClusterName", "");
		esHost = prop.getProperty("esHost", "localhost");
		esPort = Integer.parseInt(prop.getProperty("esPort", "9300"));
		esHostPortList = prop.getProperty("esHostPortList", "localhost:9300");
		esIndex = prop.getProperty("esIndex", "kafkaConsumerIndex");
		esIndexType = prop.getProperty("esIndexType", "kafka");
		esMsgFailureTolerancePercent = Integer.parseInt(prop.getProperty(
				"esMsgFailureTolerancePercent", "5"));
		logPropertyFile = prop.getProperty("logPropertyFile",
				"log4j.properties");
		isDryRun = prop.getProperty("isDryRun", "false");
		consumerSleepTime = Integer.parseInt(prop.getProperty(
				"consumerSleepTime", "25"));

		input.close();
		logger.info("Config reading complete !");
	}

}
