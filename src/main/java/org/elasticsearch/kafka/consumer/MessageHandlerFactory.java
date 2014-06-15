package org.elasticsearch.kafka.consumer;

import java.util.LinkedHashMap;

import org.elasticsearch.client.Client;


public interface MessageHandlerFactory {
	
	public MessageHandler createMessageHandler(Client client,ConsumerConfig config, LinkedHashMap<Long, ? extends Object> offsetMsgMap);
		
	public void transformMessage() throws Exception;
	
	public void prepareForPostToElasticSearch() throws Exception;
	
	public boolean postToElasticSearch() throws Exception;

	
	
}
