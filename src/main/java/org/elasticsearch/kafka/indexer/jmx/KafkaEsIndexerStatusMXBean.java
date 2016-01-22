package org.elasticsearch.kafka.indexer.jmx;

import java.util.List;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatus;

public interface KafkaEsIndexerStatusMXBean {
	boolean isAlive();
	List <IndexerJobStatus> getStatuses();
	int getCountOfFailedJobs();
	int getCountOfCancelledJobs();
	int getCountOfStoppedJobs();
	int getCountOfHangingJobs();

}
