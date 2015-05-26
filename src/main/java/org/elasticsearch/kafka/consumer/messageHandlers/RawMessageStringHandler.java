package org.elasticsearch.kafka.consumer.messageHandlers;

import java.nio.ByteBuffer;
import java.util.Iterator;

import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.kafka.consumer.BasicIndexHandler;
import org.elasticsearch.kafka.consumer.ConsumerConfig;
import org.elasticsearch.kafka.consumer.IndexHandler;
import org.elasticsearch.kafka.consumer.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawMessageStringHandler extends MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(RawMessageStringHandler.class);
	private IndexHandler indexHandler;

	public RawMessageStringHandler(TransportClient client,ConsumerConfig config){
		super(client, config);
		// for this message handler class - we can use the BasicIndexHandler since
		// there is not custom logic for index name lookup
		indexHandler = new BasicIndexHandler(this.getConfig());
		logger.info("Initialized RawMessageStringHandler");
	}
	
	public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator){
		this.setBuildReqBuilder(this.getEsClient().prepareBulk());
		int numProcessedMessages = 0;
		int numMessagesInBatch = 0;
		long offsetOfNextBatch = 0;
		while(messageAndOffsetIterator.hasNext()) {
			numMessagesInBatch++;
			MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
			offsetOfNextBatch = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			ByteBuffer payload = message.payload();
			byte[] bytesMessage = new byte[payload.limit()];
			payload.get(bytesMessage);
			byte[] transformedMessage;
			try {
				transformedMessage = this.transformMessage(bytesMessage, messageAndOffset.offset());
			} catch (Exception e) {
				// TODO decide whether you want to fail the whole batch if transformation 
				// of one message fails, or if you just want to log this message into failedEvents.log
				// for later re-processing
				// for now - just log and continue
				logger.error("ERROR transforming message at offset={} - skipping it: {}", 
						messageAndOffset.offset(), new String(bytesMessage), e);
				continue;
			}
			this.getBuildReqBuilder().add(
				this.getEsClient().prepareIndex(
					indexHandler.getIndexName(null), indexHandler.getIndexType(null))
					.setSource(transformedMessage)
			);
			numProcessedMessages++;
		}
		logger.info("Total # of messages in this batch: {};" + 
			"# of successfully transformed and added to Index messages: {}; offsetOfNextBatch: {}", 
			numMessagesInBatch, numProcessedMessages, offsetOfNextBatch);
		return offsetOfNextBatch;
	}
	
	public byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception{
		byte[] outputMessage;
		// do necessary transformation here
		// in the simplest case - post as is
		outputMessage = inputMessage;		
		return outputMessage; 		
	}

}
