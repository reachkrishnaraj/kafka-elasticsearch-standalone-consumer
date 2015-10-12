package org.elasticsearch.kafka.indexer.jmx;

import java.util.List;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobManager;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatus;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatusEnum;

public class KafkaEsIndexerStatus implements KafkaEsIndexerStatusMXBean {

	protected IndexerJobManager indexerJobManager;
	private int failedJobs;
	private int cancelledJobs;
	private int stoppedJobs;

	public KafkaEsIndexerStatus(IndexerJobManager indexerJobManager) {
		this.indexerJobManager = indexerJobManager;
	}

	public boolean isAlive() {
			return true;
	}

	public List<IndexerJobStatus> getStatuses() {
		return indexerJobManager.getJobStatuses();
	}

	public int getCountOfFailedJobs() {
		failedJobs = 0;
		for (IndexerJobStatus jobStatus : indexerJobManager.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Failed)){
				failedJobs++;
			}
		}
		return failedJobs;
	}

	public int getCountOfStoppedJobs() {
		stoppedJobs = 0;
		for (IndexerJobStatus jobStatus : indexerJobManager.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Stopped)){
				stoppedJobs++;
			}
		}
		return stoppedJobs;
	}

	public int getCountOfCancelledJobs() {
		cancelledJobs = 0;
		for (IndexerJobStatus jobStatus : indexerJobManager.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Cancelled)){
				cancelledJobs++;
			}
		}
		return cancelledJobs;
	}

}
