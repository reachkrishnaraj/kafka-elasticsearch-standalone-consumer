package org.elasticsearch.kafka.consumer.messageHandlers;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.MessageHandler;

public class RawMessageStringHandler extends MessageHandler {

	Logger logger = ConsumerLogger.getLogger(this.getClass());

	public RawMessageStringHandler(){
		super();
		logger.info("Initialized RawMessageStringHandler");
	}
		
	public void transformMessage() throws Exception{
		logger.info("Starting to transformMessages into String Messages");
		for(Long offsetKey : this.getOffsetMsgMap().keySet()){
			ByteBuffer payload = this.getOffsetMsgMap().get(offsetKey).payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
			this.getEsPostObject().add(new String(bytes, "UTF-8"));
		}
		logger.info("Completed transforming Messages into String Messages");
	}
	
	public void prepareForPostToElasticSearch(){
		logger.info("Starting prepareForPostToElasticSearch");
		BulkRequestBuilder buildReqBuilder = this.getEsClient().prepareBulk();
		for (Object eachMsg : this.getEsPostObject()){
			buildReqBuilder.add(this.getEsClient().prepareIndex(this.getConfig().esIndex, this.getConfig().esIndexType).setSource((String)eachMsg));
		}
		this.setBuildReqBuilder(buildReqBuilder);
	}

}
