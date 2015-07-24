package org.elasticsearch.kafka.indexer;

import java.util.HashMap;

/**
 * Basic interface for ElasticSearch Index name lookup 
 * 
 * getIndexName() and getIndexType() methods should be implemented as needed - to use custom logic
 * to determine which index to send the data to, based on business-specific 
 * criteria
 * 
 * @author marinapopova
 *
 */
public interface IndexHandler {
	
	// default index name, if not specified/calculated otherwise
	public static final String DEFAULT_INDEX_NAME = "test_index";
	// default index type, if not specified/calculated otherwise
	public static final String DEFAULT_INDEX_TYPE = "test_index_type";

	public String getIndexName (HashMap<String, Object> indexLookupProperties);
	public String getIndexType (HashMap<String, Object> indexLookupProperties);

}
