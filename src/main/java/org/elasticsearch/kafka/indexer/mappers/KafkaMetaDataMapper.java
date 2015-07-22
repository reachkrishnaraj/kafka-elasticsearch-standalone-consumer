package org.elasticsearch.kafka.indexer.mappers;

public class KafkaMetaDataMapper {

	private String topic;
	private String consumerGroupName;
	private short partition;
	private long offset;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}

	public short getPartition() {
		return partition;
	}

	public void setPartition(short partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

}
