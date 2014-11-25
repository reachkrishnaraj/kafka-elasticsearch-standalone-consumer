package org.elasticsearch.kafka.consumer.messageHandlers;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import kafka.message.Message;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.helpers.ExceptionHelper;
import org.elasticsearch.kafka.consumer.mappers.AccessLogMapper;


public class AccessLogMessageHandler extends RawMessageStringHandler {
	
	private final String actualDateFormat = "dd/MMM/yyyy:hh:mm:ss";
	private final String expectedDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private final String actualTimeZone = "Europe/London";
	private final String expectedTimeZone = "Europe/London";
	Logger logger = ConsumerLogger.getLogger(this.getClass());
	
	private ObjectMapper mapper = new ObjectMapper();
	private AccessLogMapper accessLogMsgObj = new AccessLogMapper();
	
	private String[] splittedMsg = null;
	private SimpleDateFormat actualFormat = null;
	private SimpleDateFormat expectedFormat = null;
	private String dateString[] = null;
	private Date date = null;
	private String[] serverAndInstance = null;
	
	public AccessLogMessageHandler(){
		super();
		logger.info("Initialied org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler");		
	}
	
	@Override
	public void transformMessage() throws Exception {
		logger.info("*** Starting to transform messages ***");
		logger.info("# of message available for this round is:" + this.getOffsetMsgMap().size());
		
		String transformedMsg;
		Long offsetKey;
		ByteBuffer payload;
		byte[] bytes;
		Map.Entry<Long,Message> keyValuePair;
		
		this.getOffsetFailedMsgMap().clear();
		this.getEsPostObject().clear();
		
		Iterator<Map.Entry<Long,Message>> offsetMsgMapItr = this.getOffsetMsgMap().entrySet().iterator();
		while (offsetMsgMapItr.hasNext()) {
			transformedMsg = "";
			keyValuePair = (Map.Entry<Long,Message>)offsetMsgMapItr.next();
			payload = keyValuePair.getValue().payload();
			bytes = new byte[payload.limit()];
			payload.get(bytes);
			offsetKey = keyValuePair.getKey();
			offsetMsgMapItr.remove();
			try{
            	//logger.info("above to convert the message to JSON");
            	transformedMsg = convertToJson(new String(bytes, "UTF-8"), offsetKey);
            	logger.debug(transformedMsg);
            }
			catch(Exception e){
				this.getOffsetFailedMsgMap().put(offsetKey, new String(bytes, "UTF-8"));
            	logger.error("Failed to transform message @ offset::" + offsetKey + "and failed message is::"+ new String(bytes, "UTF-8"));
            	logger.error("Error is::" + ExceptionHelper.getStrackTraceAsString(e));
            }
			
			if(transformedMsg != null && !transformedMsg.isEmpty()){
				this.getEsPostObject().add(transformedMsg);
			}
			keyValuePair = null;
		}
		this.sizeOfOffsetMsgMap = this.getOffsetFailedMsgMap().size();
		logger.info("**** Completed transforming all ATG10 access log messages ****");	

		//Above code will remove the message from the LinkedHashMap and hence good for memory. Need to remove the below block of code.	
	
		transformedMsg = null;
		offsetKey = null;
		payload = null;
		bytes = null;
		keyValuePair = null;
	}

	
	public String convertToJson(String rawMsg, Long offset) throws Exception{
				
		this.splittedMsg =  rawMsg.split("\\|");
		
		for(int i=0; i < this.splittedMsg.length; i++){
			 this.splittedMsg[i] = this.splittedMsg[i].trim();
		}
	
		this.accessLogMsgObj = new AccessLogMapper();
		accessLogMsgObj.setRawMessage(rawMsg);
		accessLogMsgObj.getKafkaMetaData().setOffset(offset);
		accessLogMsgObj.getKafkaMetaData().setTopic(this.getConfig().topic);
		accessLogMsgObj.getKafkaMetaData().setConsumerGroupName(this.getConfig().consumerGroupName);
		accessLogMsgObj.getKafkaMetaData().setPartition(this.getConfig().partition);
		
		accessLogMsgObj.setIp(splittedMsg[0].trim());
		accessLogMsgObj.setProtocol(splittedMsg[1].trim());
		
		
		if(splittedMsg[5].toUpperCase().contains("GET")){
			accessLogMsgObj.setIp(splittedMsg[3].trim());
			accessLogMsgObj.setProtocol(splittedMsg[4].trim());
			
			accessLogMsgObj.setMethod(splittedMsg[5].trim());
			accessLogMsgObj.setPayLoad(splittedMsg[6].trim());
			accessLogMsgObj.setResponseCode(Integer.parseInt(splittedMsg[8].trim()));
			accessLogMsgObj.setSessionID(splittedMsg[9].trim());
			this.serverAndInstance = splittedMsg[9].split("\\.")[1].split("-");

			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setServerAndInstance(serverAndInstance[0].trim() + "_" + serverAndInstance[1].trim());
			
			accessLogMsgObj.setHostName(splittedMsg[12].split(" " )[0].trim());
			accessLogMsgObj.setResponseTime(Integer.parseInt(splittedMsg[13].trim()));
			accessLogMsgObj.setUrl(splittedMsg[11].trim());
			accessLogMsgObj.setAjpThreadName(splittedMsg[14].trim());
			accessLogMsgObj.setSourceIpAndPort(null);
			
			this.actualFormat = new SimpleDateFormat(actualDateFormat);
			this.actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			this.expectedFormat = new SimpleDateFormat(expectedDateFormat);
			this.expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			this.dateString = splittedMsg[0].split(" " );
			
			this.date = actualFormat.parse(dateString[0].trim().replaceAll("\\[", "").trim());
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));
		}
		
		
		if(splittedMsg[5].toUpperCase().contains("POST")){
			accessLogMsgObj.setIp(splittedMsg[3].trim());
			accessLogMsgObj.setProtocol(splittedMsg[4].trim());
			
			accessLogMsgObj.setMethod(splittedMsg[5].trim());
			if(!splittedMsg[6].trim().isEmpty()){
				accessLogMsgObj.setPayLoad(splittedMsg[6].trim());
			}
			accessLogMsgObj.setResponseCode(Integer.parseInt(splittedMsg[8].trim()));
			accessLogMsgObj.setSessionID(splittedMsg[9].trim());
			this.serverAndInstance = splittedMsg[9].split("\\.")[1].split("-");

			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setServerAndInstance(serverAndInstance[0].trim() + "_" + serverAndInstance[1].trim());
			
			accessLogMsgObj.setHostName(splittedMsg[12].trim().split(" " )[0]);
			accessLogMsgObj.setResponseTime(Integer.parseInt(splittedMsg[13].trim()));
			accessLogMsgObj.setUrl(splittedMsg[11].trim());
			accessLogMsgObj.setAjpThreadName(splittedMsg[14].trim());
			accessLogMsgObj.setSourceIpAndPort(null);
			
			actualFormat = new SimpleDateFormat(actualDateFormat);
			actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			expectedFormat = new SimpleDateFormat(expectedDateFormat);
			expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			this.dateString = splittedMsg[0].split(" " );
			
			this.date = actualFormat.parse(dateString[0].trim().replaceAll("\\[", "").trim());
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));		
		}
		
		//Freeing Up these folks
		this.splittedMsg = null;
		this.actualFormat = null;
		this.expectedFormat = null;
		this.dateString = null;
		this.date = null;
		this.serverAndInstance = null;
		
	return mapper.writeValueAsString(accessLogMsgObj);		
	
	}

	
	
}
