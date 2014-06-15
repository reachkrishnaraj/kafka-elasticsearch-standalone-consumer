package org.elasticsearch.kafka.consumer.daemon;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.elasticsearch.kafka.consumer.ConsumerConfig;
import org.elasticsearch.kafka.consumer.ConsumerJob;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.helpers.ExceptionHelper;
import org.apache.log4j.Logger;

public class KafkaConsumerDaemon implements Daemon {
	
	private Thread kafkaConsumerThread; 
	private boolean stopped = false;
	public ConsumerJob kafkaConsumerJob = null;
	private boolean isConsumeJobInProgress = false;
	Logger logger;
	
	@Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
		String[] args = daemonContext.getArguments();
		System.out.println("Arguments passed to the Consumer Daemon are::");
		for(String arg : args){
			System.out.println(arg);
		}
		//System.setProperty("log4j.configuration", "file://" + args[1]);
		//this.logger.info("Initializing the Kafka Consumer ...");
		//System.out.println("Initializing the Kafka Consumer ...");
		
		ConsumerConfig kafkaConsumerConfig = new ConsumerConfig(args[0]);
		ConsumerLogger.doInitLogger(kafkaConsumerConfig);
		this.logger = ConsumerLogger.getLogger(this.getClass());
		this.logger.info("Created the kafka consumer config ...");
		System.out.println("Created the kafka consumer config ...");
		try{
			kafkaConsumerJob = new ConsumerJob(kafkaConsumerConfig);
		}
		catch(Exception e){
			this.logger.fatal("Exception happened when trying to Initialize ConsumerJob object");
			e.printStackTrace();
			this.logger.fatal("******* NOT able to start Consumer Daemon *******");
			this.logger.fatal(ExceptionHelper.getStrackTraceAsString(e));
			System.out.println("Exception happened when trying to Initialize ConsumerJob object");
			System.out.println("******* NOT able to start Consumer Daemon *******");
			throw e;
		}
		
		this.logger.info("Successfully created the kafka consumer client. Starting the daemon now ...");
		System.out.println("Successfully created the kafka consumer client. Starting the daemon now ...");
		
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
                		//this.logger.info("starting to wait");
                		isConsumeJobInProgress = true;
                		//kafkaConsumerThread.sleep(30000);
                		//this.logger.info("Completed waiting for the inprogress Consumer Job");
                		kafkaConsumerJob.doRun();
                		isConsumeJobInProgress = false;
                		logger.info("Completed a round of kafka consumer job");
                	}
                	catch(Exception e){
                		isConsumeJobInProgress = false;
                		logger.fatal("Exception occured when starting a new round of kafka consumer job. Exception is:");
                		logger.fatal(ExceptionHelper.getStrackTraceAsString(e));
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
		this.logger.info("Trying to start the Consumer Daemon");
		try{
			kafkaConsumerThread.start();
		}
		catch(Exception e){
			System.out.println("********** Exception when trying to start the Consumer Daemon **********");
			e.printStackTrace();
			logger.fatal("Exception when starting the Consumer Daemon." + ExceptionHelper.getStrackTraceAsString(e));
		}
		
    }
	
	@Override
    public void stop() throws Exception {
		this.logger.info("Received the stop signal, trying to start the Consumer Daemon");
		System.out.println("Received the stop signal, trying to start the Consumer Daemon");
		stopped = true;
        while(isConsumeJobInProgress){
        	this.logger.info(".... Waiting for inprogress Consumer Job to complete ...");
        	Thread.sleep(1000);
        }
        /*try{
        	kafkaConsumerThread.join(1000);
        }catch(InterruptedException e){
            System.err.println(e.getMessage());
            throw e;
        }*/
        this.logger.info("Completed waiting for inprogess Consumer Job to finish. Stopping the Consumer....");
        try{
        	kafkaConsumerJob.stop();
        }
        catch(Exception e){
        	System.out.println("********** Exception when trying to stop the Consumer Daemon **********");
			e.printStackTrace();
			logger.fatal("Exception when stopping the Consumer Daemon." + ExceptionHelper.getStrackTraceAsString(e));
        }
        this.logger.info("Stopped the Consumer Job");
        System.out.println("Stopped the Consumer Job");
        
    }
	
	@Override
    public void destroy() {
		//kafkaConsumerJob.stop();
		kafkaConsumerThread = null;
		this.logger.info("Completed destroying the objects.. clean exit ..");
    }
	
}
