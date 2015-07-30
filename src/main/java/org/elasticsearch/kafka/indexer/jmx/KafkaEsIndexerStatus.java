package org.elasticsearch.kafka.indexer.jmx;

public class KafkaEsIndexerStatus implements KafkaEsIndexerStatusMBean{
	
	public boolean isAlive() {		
		return true;
	}

}
