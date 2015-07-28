package org.elasticsearch.kafka.indexer.jmx;

public class KafkaEsIndexerStatus implements KafkfaEsIndexerStatusMBean{

	
	public boolean isAlive() {
		
		return true;
	}

}
