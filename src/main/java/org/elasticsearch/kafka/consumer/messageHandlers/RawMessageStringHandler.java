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
		this.getEsPostObject().clear();
		Iterator<Map.Entry<Long,Message>> offsetMsgMapItr = this.getOffsetMsgMap().entrySet().iterator();
		while (offsetMsgMapItr.hasNext()) {
			Map.Entry<Long,Message> keyValuePair = (Map.Entry<Long,Message>)offsetMsgMapItr.next();
			ByteBuffer payload = keyValuePair.getValue().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			// TODO consider using byte[] array directly - without converting to String
			this.getEsPostObject().add(new String(bytes, "UTF-8"));
		}
	}
	
	public void prepareForPostToElasticSearch(){
		this.setBuildReqBuilder(this.getEsClient().prepareBulk());
		Iterator<Object> esPostObjItr = this.getEsPostObject().iterator();
		while(esPostObjItr.hasNext()) {
			String eachMsg = (String) esPostObjItr.next();
			// TODO remove all unnecessary this.xxx and get() calls - for clarity
			this.getBuildReqBuilder().add(
					this.getEsClient().prepareIndex(
							this.getConfig().esIndex, this.getConfig().esIndexType)
							.setSource((String)eachMsg)
							);
		}
		this.getEsPostObject().clear();
	}

}
