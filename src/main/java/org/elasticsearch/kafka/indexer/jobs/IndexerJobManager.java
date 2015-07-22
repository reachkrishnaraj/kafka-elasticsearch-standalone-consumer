package org.elasticsearch.kafka.indexer.jobs;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerJobManager {

	private static final Logger logger = LoggerFactory.getLogger(IndexerJobManager.class);
	private ConsumerConfig consumerConfig;
	private TransportClient esClient;
    private ExecutorService executorService;
    private int numOfPartitions;
    private int firstPartition;
    private int lastPartition;
    // Map of <partitionNumber, IndexerJob> of indexer jobs for all partitions
    private ConcurrentHashMap<Integer, IndexerJob> indexerJobs;
    // List of <Future<IndexerJobStatus>> futures of all submitted indexer jobs for all partitions
    private List<Future<IndexerJobStatus>> indexerJobFutures;

	public IndexerJobManager(ConsumerConfig config) throws Exception {
		this.consumerConfig = config;
		firstPartition = config.firstPartition;
		lastPartition = config.lastPartition;
		numOfPartitions = lastPartition - firstPartition + 1;
		if (numOfPartitions <= 0) {
			logger.error("ERROR in configuration: number of partitions is <= 0");
			throw new Exception("ERROR in configuration: number of partitions is <= 0");
		}
		logger.info("ConsumerJobManager is starting, servicing partitions: [{}-{}]",
				firstPartition, lastPartition);
	}

	public void startAll() throws Exception {
		initElasticSearch();
        executorService = Executors.newFixedThreadPool(numOfPartitions);
        indexerJobs = new ConcurrentHashMap<>();
		// create as many IndexerJobs as there are partitions in the events topic
        // first create all jobs without starting them - to make sure they can init all resources OK
        try {
	        for (int partition=firstPartition; partition<=lastPartition; partition++){
	        	logger.info("Creating IndexerJob for partition={}", partition);
	        	IndexerJob pIndexerJob = new IndexerJob(consumerConfig, partition, esClient);
	        	indexerJobs.put(partition, pIndexerJob);
	        }
        } catch (Exception e) {
        	logger.error("ERROR: Failure creating a consumer job, exiting: ", e);
        	// if any job startup fails - abort; 
        	throw e;
        }
        // now start them all
        indexerJobFutures = executorService.invokeAll(indexerJobs.values());       
	}
	
	private void initElasticSearch() throws Exception {
		String[] esHostPortList = consumerConfig.esHostPortList.trim().split(",");
		logger.info("Initializing ElasticSearch... hostPortList={}, esClusterName={}",
			consumerConfig.esHostPortList, consumerConfig.esClusterName);

		// TODO add validation of host:port syntax - to avoid Runtime exceptions
		try {
			Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", consumerConfig.esClusterName)
				.build();
			esClient = new TransportClient(settings);
			for (String eachHostPort : esHostPortList) {
				logger.info("adding [{}] to TransportClient ... ", eachHostPort);
				esClient.addTransportAddress(
					new InetSocketTransportAddress(
						eachHostPort.split(":")[0].trim(), 
						Integer.parseInt(eachHostPort.split(":")[1].trim())
					)
				);
			}
			logger.info("ElasticSearch Client created and intialized OK");
		} catch (Exception e) {
			logger.error("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::"
					+ e.getMessage());
			throw e;
		}
	}

	public void getJobStatuses(){
		// TODO check all jobs and return a list of IndexerJobStatus'es
	}
	
	public void stop() {
		logger.info("About to stop ElasticSearch Client and all consumer jobs ...");
		if (esClient != null)
			esClient.close();
		if (executorService != null && !executorService.isTerminated()) {
			try {
				executorService.awaitTermination(consumerConfig.appStopTimeoutSeconds, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("ERROR: failed to stop all consumer jobs due to InterruptedException: ", e);
			}
		}
		logger.info("Stop() finished OK");
	}

}
