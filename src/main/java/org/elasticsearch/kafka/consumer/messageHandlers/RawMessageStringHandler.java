package org.elasticsearch.kafka.consumer.messageHandlers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import kafka.message.Message;

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
		logger.info("# of message available for this round is:" + this.getOffsetMsgMap().size());

		this.getEsPostObject().clear();
		Iterator<Map.Entry<Long,Message>> offsetMsgMapItr = this.getOffsetMsgMap().entrySet().iterator();
		while (offsetMsgMapItr.hasNext()) {
			Map.Entry<Long,Message> keyValuePair = (Map.Entry<Long,Message>)offsetMsgMapItr.next();
			ByteBuffer payload = keyValuePair.getValue().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			this.getEsPostObject().add(new String(bytes, "UTF-8"));
			offsetMsgMapItr.remove();
		}

		logger.info("Completed transforming Messages into String Messages");
	}
	
	public void prepareForPostToElasticSearch(){
		logger.info("Starting prepareForPostToElasticSearch");
		this.setBuildReqBuilder(this.getEsClient().prepareBulk());
		logger.info("Completed constructing buildReqBuilder for ES");
		Iterator<Object> esPostObjItr = this.getEsPostObject().iterator();
		while(esPostObjItr.hasNext()) {
			String eachMsg = (String) esPostObjItr.next();
			this.getBuildReqBuilder().add(this.getEsClient().prepareIndex(this.getConfig().esIndex, this.getConfig().esIndexType).setSource((String)eachMsg));
			esPostObjItr.remove();
		}
		this.getEsPostObject().clear();
		//Above code will remove the message from the ArrayList and hence good for memory. Need to remove the below block of code.
		
		logger.info("Completed setting the messages in the buildReqBuilder for ES");
	}

}
