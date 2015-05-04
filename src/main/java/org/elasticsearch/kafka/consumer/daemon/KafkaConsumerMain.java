package org.elasticsearch.kafka.consumer.daemon;

import org.apache.log4j.Logger;
import org.elasticsearch.kafka.consumer.ConsumerConfig;
import org.elasticsearch.kafka.consumer.ConsumerJob;
import org.elasticsearch.kafka.consumer.ConsumerLogger;

public class KafkaConsumerMain {
	
	private boolean stopped = false;
	public ConsumerJob kafkaConsumerJob = null;
	private boolean isConsumeJobInProgress = false;
	private Logger logger;
	private ConsumerConfig kafkaConsumerConfig;
	
	public KafkaConsumerMain(){		
	}
	
    public void init(String[] args) throws Exception {
		System.out.println("Initializing the Kafka Consumer config, arguments passed to the Driver: ");
		for(String arg : args){
			System.out.println(arg);
		}
		
		kafkaConsumerConfig = new ConsumerConfig(args[0]);
		ConsumerLogger.doInitLogger(kafkaConsumerConfig);
		logger = ConsumerLogger.getLogger(this.getClass());
		logger.info("Created the kafka consumer config, about to create a new Kafka Consumer Job");
		System.out.println("Created the kafka consumer config, about to create a new Kafka Consumer Job");
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
        		Thread.sleep(kafkaConsumerConfig.consumerSleepTime * 1000);
        		logger.debug("Completed a round of kafka consumer job");
        	} catch (InterruptedException e) {
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
		System.out.println("Received the stop signal, trying to stop the Consumer job");
		stopped = true;
        while(isConsumeJobInProgress){
        	this.logger.info(".... Waiting for inprogress Consumer Job to complete ...");
        	Thread.sleep(1000);
        }
        logger.info("Completed waiting for inprogess Consumer Job to finish - stopping the job");
        try{
        	kafkaConsumerJob.stop();
        }
        catch(Exception e){
        	System.out.println("********** Exception when trying to stop the Consumer Job: " + 
        			e.getMessage());
			e.printStackTrace();
        }
        logger.info("Stopped the Consumer Job");
        System.out.println("Stopped the Consumer Job");
        
    }
	
    public static void main(String[] args) {
    	KafkaConsumerMain driver = new KafkaConsumerMain();
    	try {
			driver.init(args);
			driver.start();
		} catch (Exception e) {
			System.out.println("Exception from main() - exiting: " + e.getMessage());
		}
    	
    }
}
