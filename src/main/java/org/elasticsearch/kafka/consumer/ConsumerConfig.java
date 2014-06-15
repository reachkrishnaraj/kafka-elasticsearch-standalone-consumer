package org.elasticsearch.kafka.consumer;

import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.common.unit.TimeValue;

public class ConsumerConfig {

	Properties prop = new Properties();
	InputStream input = null;
	//Logger object cannot be initialized since the logProperty file for the instance would be known only after config is read
	//Logger logger = ConsumerLogger.initLogger(this.getClass());
	private final int BULK_MSG_SIZE = 10 * 1024 * 1024 * 3;
	private final int BULK_MSG_TIMEOUT = 10000;
	private final String BULK_MSG_TIMEOUT_STRING = "10ms";	
	
	//Kafka ZooKeeper's IP Address/HostName without port
	public final String zookeeper;
	//Full class path and name for the concrete message handler class factory
	public final String messageHandlerClass;
	//Kafka Broker's IP Address/HostName
	public final String brokerHost;
	//Kafka Broker's Port number
	public final int brokerPort;
	//Kafka Topic from which the message has to be processed
	public final String topic;
	//Partition in the Kafka Topic from which the message has to be processed 
	public final short partition;
	//Option from where the message fetching should happen in Kafka
	// Values can be: CUSTOM/OLDEST/LATEST/RESTART.
	// If 'CUSTOM' is set, then 'startOffset' has to be set an int value
	public final String startOffsetFrom;
	//int value of the offset from where the message processing should happen
	public final int startOffset;
	//Name of the Kafka Consumer Group
	public final String consumerGroupName;
	public final String statsdPrefix;
	public final String statsdHost;
	public final int statsdPort;
	//Preferred Size of message to be fetched from Kafka in 1 Fetch call to kafka 
	public final int bulkSize;
	//Timeout when fetching message from Kafka
	public final TimeValue bulkTimeout;
	//Preferred Message Encoding to process the message before posting it to ElasticSearch
	public final String messageEncoding;
	//TBD
	public final boolean isGuranteedEsPostMode;
	//Name of the ElasticSearch Cluster
	public final String esClusterName;
	//Hostname/ipAddress of ElasticSearch
	public final String esHost;
	//Port number of ElasticSearch
	public final int esPort;
	//IndexName in ElasticSearch to which the processed Message has to be posted
	public final String esIndex;
	//IndexType in ElasticSearch to which the processed Message has to be posted
	public final String esIndexType;
	//Percentage of message failure tolerance
	public final int esMsgFailureTolerancePercent;
	
	//Log property file for the consumer instance
	public final String logPropertyFile;
	

	public ConsumerConfig(String configFile) throws Exception {
		try {
			//logger.info("configFile Passed::"+configFile);
			input = this.getClass().getClassLoader().getResourceAsStream(configFile);
			//logger.info("configFile loaded Successfully");
			System.out.println("configFile loaded Successfully");
		} catch (Exception e) {
			//logger.fatal("Error reading/loading ConfigFile. Throwing the error. Error Message::" + e.getMessage());
			System.out.println("Error reading/loading ConfigFile. Throwing the error. Error Message::" + e.getMessage());
			e.printStackTrace();
			throw e;
		} 
		
		if (input != null) {
			System.out.println("configFile NOT loaded Successfully.Hence reading the default values for the properties");
			// load the properties file
			prop.load(input);
			zookeeper = (String) prop.getProperty("zookeeper", "localhost");
			messageHandlerClass = prop.getProperty("messageHandlerClass", "org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler");
			brokerHost = prop.getProperty("brokerHost", "localhost");
			brokerPort = Integer.parseInt(prop.getProperty("brokerPort", "9092"));
			topic = prop.getProperty("topic", "");
			partition = Short.parseShort(prop.getProperty("partition", "0"));
			startOffsetFrom = prop.getProperty("startOffsetFrom", "");
			startOffset = Integer.parseInt(prop.getProperty("startOffset", ""));
			consumerGroupName = prop.getProperty("consumerGroupName", "ESKafkaConsumerClient");
			statsdPrefix = prop.getProperty("statsdPrefix", "");
			statsdHost = prop.getProperty("statsdHost", "");
			statsdPort = Integer.parseInt(prop.getProperty("statsdPort", "0"));
			bulkSize = Integer.parseInt(prop.getProperty("bulkSize",
					String.valueOf(BULK_MSG_SIZE)));
			bulkTimeout = TimeValue.parseTimeValue(
					(prop.getProperty("bulkTimeout", BULK_MSG_TIMEOUT_STRING)),
					TimeValue.timeValueMillis(BULK_MSG_TIMEOUT));
			messageEncoding = prop.getProperty("messageEncoding", "UTF-8");
			isGuranteedEsPostMode = Boolean.getBoolean(prop.getProperty("isGuranteedEsPostMode", "false"));
			
			esClusterName = prop.getProperty("esClusterName", "");
			esHost = prop.getProperty("esHost", "localhost");
			esPort = Integer.parseInt(prop.getProperty("esPort", "9300"));
			esIndex = prop.getProperty("esIndex", "kafkaConsumerIndex");
			esIndexType = prop.getProperty("esIndexType", "kafka");
			esMsgFailureTolerancePercent = Integer.parseInt(prop.getProperty("esMsgFailureTolerancePercent", "5"));
			logPropertyFile = prop.getProperty("logPropertyFile", "log4j.properties");
			
		} else {
			zookeeper = "localhost";
			messageHandlerClass = "";
			brokerHost = "";
			brokerPort = 0;
			topic = "";
			partition = 0;
			startOffsetFrom = "";
			startOffset = 0;
			consumerGroupName = "";
			statsdPrefix = "";
			statsdHost = "";
			statsdPort = 0;
			bulkSize = BULK_MSG_SIZE;
			bulkTimeout = TimeValue.timeValueMillis(BULK_MSG_TIMEOUT);
			messageEncoding = "UTF-8";
			isGuranteedEsPostMode = false;
			esClusterName = "elasticsearch";
			esHost = "localhost";
			esPort = 9200;
			esIndex = "kafkaConsumerIndex";
			esIndexType = "kafka";
			esMsgFailureTolerancePercent = 5;
			logPropertyFile = "log4j.properties";
		}
	input.close();
	System.out.println("Config reading complete !");
	//logger.info("configFile loaded,read and closed Successfully");
	}

	
	
}
