package org.elasticsearch.kafka.indexer.jmx;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatusEnum;

public interface IndexerJobStatusMBean {
	long getLastCommittedOffset();
	IndexerJobStatusEnum getJobStatus();
	int getPartition();
}
