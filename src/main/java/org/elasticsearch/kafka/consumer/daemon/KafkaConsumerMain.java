package org.elasticsearch.kafka.consumer.daemon;

import java.time.LocalDateTime;

import org.elasticsearch.kafka.consumer.ConsumerConfig;
import org.elasticsearch.kafka.consumer.ConsumerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerMain {
	
	private boolean stopped = false;
	public ConsumerJob kafkaConsumerJob = null;
	private boolean isConsumeJobInProgress = false;
	private ConsumerConfig kafkaConsumerConfig;
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerMain.class);
	
	public KafkaConsumerMain(){		
	}
	
    public void init(String[] args) throws Exception {
		logger.info("Initializing the Kafka Consumer config, arguments passed to the Driver: ");
		for(String arg : args){
			logger.info(arg);
		}		
		kafkaConsumerConfig = new ConsumerConfig(args[0]);
		logger.info("Created the kafka consumer config, about to create a new Kafka Consumer Job");
		kafkaConsumerJob = new ConsumerJob(kafkaConsumerConfig);		
    }
	

    public void start() throws Exception {
        while(!stopped){
        	try{
        		// check if there was a request to stop this thread - stop processing if so
                if (Thread.currentThread().isInterrupted()){
                    // preserve interruption state of the thread
                    Thread.currentThread().interrupt();
                    throw new InterruptedException("Cought interrupted event in start() - stopping");
                }
        		logger.info("******* Starting a new round of kafka consumer job");
        		isConsumeJobInProgress = true;
        		kafkaConsumerJob.doRun();
        		isConsumeJobInProgress = false;
        		// sleep for configured time
        		Thread.sleep(kafkaConsumerConfig.consumerSleepBetweenFetchsMs * 1000);
        		logger.debug("Completed a round of kafka consumer job");
        	} catch (InterruptedException e) {
        		isConsumeJobInProgress = false;
        		this.stop();
        	} catch(Exception e){
        		isConsumeJobInProgress = false;
        		logger.error("Exception when starting a new round of kafka consumer job, exiting: " + 
        				e.getMessage(), e);
        		// do not keep going if a severe error happened - stop and fix the issue manually,
        		// then restart the consumer again; It is better to monitor the job externally 
        		// via Zabbix or the likes - rather then keep failing [potentially] forever
        		this.stop();
        	}       
        }
		
    }
	
    public void stop() throws Exception {
		logger.info("Received the stop signal, trying to stop the Consumer job");
		stopped = true;
		
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
        	kafkaConsumerJob.stop();
        }
        catch(Exception e){
        	logger.error("********** Exception when trying to stop the Consumer Job: " + 
        			e.getMessage(), e);
			e.printStackTrace();
        }
        logger.info("Stopped the Consumer Job");
    }
	
    public static void main(String[] args) {
    	KafkaConsumerMain driver = new KafkaConsumerMain();

    	Runtime.getRuntime().addShutdownHook(new Thread() {
  	      public void run() {
  	        System.out.println("Running Shutdown Hook .... ");
  	        try {
					driver.stop();
				} catch (Exception e) {
					System.out.println("Error stopping the Consumer from the ShutdownHook: " + e.getMessage());
					e.printStackTrace();
				}
  	      }
  	   });

    	try {
			driver.init(args);
			driver.start();
		} catch (Exception e) {
			System.out.println("Exception from main() - exiting: " + e.getMessage());
		}
    	
    	
    }
}
