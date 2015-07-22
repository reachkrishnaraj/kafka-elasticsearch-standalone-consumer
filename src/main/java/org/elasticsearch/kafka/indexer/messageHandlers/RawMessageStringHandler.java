package org.elasticsearch.kafka.indexer.messageHandlers;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.elasticsearch.kafka.indexer.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawMessageStringHandler extends MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(RawMessageStringHandler.class);

	public RawMessageStringHandler(TransportClient client,ConsumerConfig config) throws Exception{
		super(client, config);
		logger.info("Initialized RawMessageStringHandler");
	}
		
	@Override
	public byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception{
		byte[] outputMessage;
		// do necessary transformation here
		// in the simplest case - post as is
		outputMessage = inputMessage;		
		return outputMessage; 		
	}

}
