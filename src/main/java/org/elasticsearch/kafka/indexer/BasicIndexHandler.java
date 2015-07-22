package org.elasticsearch.kafka.indexer;

import java.util.HashMap;

/**
 * Basic Index handler that returns ElasticSearch index name defined 
 * in the configuration file as is
 * 
 * @author marinapopova
 *
 */
public class BasicIndexHandler implements IndexHandler {

	private final ConsumerConfig config;
	private String indexName;
	private String indexType;
	
	public BasicIndexHandler(ConsumerConfig config) {
		this.config = config;
		indexName = config.esIndex;
		if (indexName == null || indexName.trim().length() < 1)
			indexName = DEFAULT_INDEX_NAME;
		indexType = config.esIndexType;
		if (indexType == null || indexType.trim().length() < 1)
			indexType =  DEFAULT_INDEX_TYPE;

	}

	@Override
	public String getIndexName(HashMap<String, Object> indexLookupProperties) {
		return indexName;
	}

	@Override
	public String getIndexType(HashMap<String, Object> indexLookupProperties) {
		return indexType;
	}

}
