package org.elasticsearch.kafka.indexer.jobs;

import org.elasticsearch.kafka.indexer.jmx.IndexerJobStatusMBean;

public class IndexerJobStatus implements IndexerJobStatusMBean{

	private long lastCommittedOffset;
	private IndexerJobStatusEnum jobStatus;
	private final int partition;

	public IndexerJobStatus(long lastCommittedOffset,
			IndexerJobStatusEnum jobStatus, int partition) {
		super();
		this.lastCommittedOffset = lastCommittedOffset;
		this.jobStatus = jobStatus;
		this.partition = partition;
	}

	public long getLastCommittedOffset() {
		return lastCommittedOffset;
	}

	public void setLastCommittedOffset(long lastCommittedOffset) {
		this.lastCommittedOffset = lastCommittedOffset;
	}

	public IndexerJobStatusEnum getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(IndexerJobStatusEnum jobStatus) {
		this.jobStatus = jobStatus;
	}

	public int getPartition() {
		return partition;
	}

	@Override
	public String toString() {
		StringBuilder sb  = new StringBuilder();
		sb.append("[IndexerJobStatus: {");
		sb.append("partition=" + partition);
		sb.append("lastCommittedOffset=" + lastCommittedOffset);
		sb.append("jobStatus=" + jobStatus.name());
		sb.append("}]");
		return sb.toString();
	}
	
}
