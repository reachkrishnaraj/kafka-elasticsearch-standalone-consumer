package org.elasticsearch.kafka.consumer.daemon;

import java.time.LocalDateTime;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.elasticsearch.kafka.consumer.ConsumerConfig;
import org.elasticsearch.kafka.consumer.ConsumerJob;
//import org.elasticsearch.kafka.consumer.ConsumerLogger;
//import org.apache.log4j.Logger;
import org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDaemon implements Daemon {
	
	private static final Logger logger = LoggerFactory.getLogger(AccessLogMessageHandler.class);
	private Thread kafkaConsumerThread; 
	private boolean stopped = false;
	public ConsumerJob kafkaConsumerJob = null;
	private boolean isConsumeJobInProgress = false;
	private static int timeLimitToStopConsumerJob = 10;
	
	@Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
		String[] args = daemonContext.getArguments();
		logger.info("Arguments passed to the Consumer Daemon are::");
		for(String arg : args){
			logger.info(arg);
		}
		//System.setProperty("log4j.configuration", "file://" + args[1]);
		//logger.info("Initializing the Kafka Consumer ...");
		//System.out.println("Initializing the Kafka Consumer ...");
		
		ConsumerConfig kafkaConsumerConfig = new ConsumerConfig(args[0]);
		//ConsumerLogger.doInitLogger(kafkaConsumerConfig);
		//logger = ConsumerLogger.getLogger(this.getClass());
		logger.info("Created the kafka consumer config ...");
		try{
			kafkaConsumerJob = new ConsumerJob(kafkaConsumerConfig);
		}
		catch(Exception e){
			logger.error("Exception happened when trying to Initialize ConsumerJob object: ");
			e.printStackTrace();
			logger.error("******* NOT able to start Consumer Daemon: " + e.getMessage(), e);
			throw e;
		}
		
		logger.info("Successfully created the kafka consumer client. Starting the daemon now ...");		
		kafkaConsumerThread = new Thread(){            
            
			@Override
            public synchronized void start() {
            	KafkaConsumerDaemon.this.stopped = false;
                super.start();
            }

            @Override
            public void run() {             
                while(!stopped){
                	try{
                		logger.info("Starting a new round of kafka consumer job");
                		//logger.info("starting to wait");
                		isConsumeJobInProgress = true;
                		//kafkaConsumerThread.sleep(30000);
                		//logger.info("Completed waiting for the inprogress Consumer Job");
                		kafkaConsumerJob.doRun();
                		isConsumeJobInProgress = false;
                		logger.info("Completed a round of kafka consumer job");
                	}
                	catch(Exception e){
                		isConsumeJobInProgress = false;
                		logger.error("Exception starting a new round of kafka consumer job: " + e.getMessage(), e);
                		logger.info("Sleeping for 5 Seconds ....");
                		try {
							Thread.sleep(5000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
                		
                	}       
                }
            }
        };
    }
	

	@Override
    public void start() throws Exception {
		logger.info("Trying to start the Consumer Daemon");
		try{
			kafkaConsumerThread.start();
		}
		catch(Exception e){
			e.printStackTrace();
			logger.error("Exception when starting the Consumer Daemon: " +e.getMessage(), e);
		}
		
    }
	
	@Override
	 public void stop() throws Exception {
		logger.info("Received the stop signal, trying to stop the Consumer job");
		stopped = true;
		
		LocalDateTime stopTime= LocalDateTime.now();

	
		
		while(isConsumeJobInProgress){
        	logger.info(".... Waiting for inprogress Consumer Job to complete ...");
        	Thread.sleep(1000);
         	LocalDateTime dateTime2= LocalDateTime.now();
        	if (java.time.Duration.between(stopTime, dateTime2).getSeconds() > timeLimitToStopConsumerJob){
        		logger.info(".... Consumer Job not responding for " + timeLimitToStopConsumerJob +" seconds - stopping the job");
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
	
	@Override
    public void destroy() {
		//kafkaConsumerJob.stop();
		kafkaConsumerThread = null;
		logger.info("Completed destroying the objects.. clean exit ..");
    }
	
}
