package org.elasticsearch.kafka.indexer;

import org.elasticsearch.kafka.indexer.jmx.KafkaEsIndexerStatus;
import org.elasticsearch.kafka.indexer.jmx.KafkaEsIndexerStatusMBean;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class KafkaIndexerDriver {
	
	boolean stopped = false;
	public IndexerJobManager indexerJobManager = null;
	private boolean isConsumeJobInProgress = false;
	private ConsumerConfig kafkaConsumerConfig;
	private static final Logger logger = LoggerFactory.getLogger(KafkaIndexerDriver.class);
	private static final String KAFKA_CONSUMER_SHUTDOWN_THREAD = "kafka-indexer-shutdown-thread";
	
	public KafkaIndexerDriver(){		
	}
	
    public void init(String[] args) throws Exception {
		logger.info("Initializing Kafka ES Indexer, arguments passed to the Driver: ");
		for(String arg : args){
			logger.info(arg);
		}		
		kafkaConsumerConfig = new ConsumerConfig(args[0]);
		logger.info("Created kafka consumer config OK");
		indexerJobManager = new IndexerJobManager(kafkaConsumerConfig);	
		
		logger.info("Registering KafkfaEsIndexerStatus MBean: ");
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
        ObjectName name = new ObjectName("org.elasticsearch.kafka.indexer:type=KafkfaEsIndexerStatus"); 
        KafkaEsIndexerStatusMBean hc = new KafkaEsIndexerStatus();
        mbs.registerMBean(hc, name); 
    }
	

    public void start() throws Exception {
    	indexerJobManager.startAll();
    }
	
    public void stop() throws Exception {
		logger.info("Received the stop signal, trying to stop all indexer jobs...");
		stopped = true;
		
		indexerJobManager.stop();
		// TODO check if we still need the forced/timed-out shutdown
		/*
		LocalDateTime stopTime= LocalDateTime.now();		
		while(isConsumeJobInProgress){
        	logger.info(".... Waiting for inprogress Consumer Job to complete ...");
        	Thread.sleep(1000);
         	LocalDateTime dateTime2= LocalDateTime.now();
        	if (java.time.Duration.between(stopTime, dateTime2).getSeconds() > kafkaConsumerConfig.timeLimitToStopConsumerJob){
        		logger.info(".... Consumer Job not responding for " + kafkaConsumerConfig.timeLimitToStopConsumerJob +" seconds - stopping the job");
        		break;
        	}
       
        }
        logger.info("Completed waiting for inprogess Consumer Job to finish - stopping the job");
        try{
        	kafkaConsumerJob.stopKafkaClient();
        }
        catch(Exception e){
        	logger.error("********** Exception when trying to stop the Consumer Job: " + 
        			e.getMessage(), e);
			e.printStackTrace();
        }
        /* */
		
        logger.info("Stopped all indexer jobs OK");
    }
	
    public static void main(String[] args) {
    	KafkaIndexerDriver driver = new KafkaIndexerDriver();

    	Runtime.getRuntime().addShutdownHook(new Thread(KAFKA_CONSUMER_SHUTDOWN_THREAD) {
  	      public void run() {
  	        logger.info("Running Shutdown Hook .... ");
  	        try {
					driver.stop();
				} catch (Exception e) {
					logger.error("Error stopping the Consumer from the ShutdownHook: " + e.getMessage());
				}
  	      }
  	   });

    	try {
			driver.init(args);
			driver.start();
		} catch (Exception e) {
			logger.error("Exception from main() - exiting: " + e.getMessage());
		}
    	
    	
    }
}
