package org.elasticsearch.kafka.consumer.messageHandlers;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import kafka.consumer.ConsumerConfig;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.mappers.AccessLogMapper;


public class AccessLogMessageHandler extends RawMessageStringHandler {
	
	
	/* This Message Handler class is an Example to show how Messahe Handler Class can be implemented
	 * This class modifies each line in the Access Log(from Apache WebServer) to desired JSON message as defined by the org.elasticsearch.kafka.consumer.mappers.AccessLogMapper class
	 */
	
	
	private final String actualDateFormat = "dd/MMM/yyyy:hh:mm:ss";
	private final String expectedDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private final String actualTimeZone = "Europe/London";
	private final String expectedTimeZone = "Europe/London";
	
	
	public AccessLogMessageHandler(){
		super();		
	}
	
	@Override
	public void transformMessage() throws Exception {
		ConsumerConfig.logger().info("*** Starting to transform messages ***");
		
		String transformedMsg;
		for(Long offsetKey : this.getOffsetMsgMap().keySet()){
			transformedMsg = "";
			ByteBuffer payload = this.getOffsetMsgMap().get(offsetKey).payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            
            try{
            	transformedMsg = convertToJson(new String(bytes, "UTF-8"), offsetKey);
            }
            catch(Exception e){
            	ConsumerLogger.logger.error("Failed to transform message @ offset::" + offsetKey + "and failed message is::"+ new String(bytes, "UTF-8"));
            	ConsumerLogger.logger.error("Error is::" + e.getMessage());
            	//e.printStackTrace();
            	//continue;
            	//Make an entry in log for the failed to process log message
            }
			if(transformedMsg != null && !transformedMsg.isEmpty()){
				this.getEsPostObject().add(transformedMsg);
			}
		}
		ConsumerConfig.logger().info("**** Completed transforming access log messages ****");	
		
	}

	
	private String convertToJson(String rawMsg, Long offset) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		String[] splittedMsg =  rawMsg.split(" ");

		AccessLogMapper accessLogMsgObj = new AccessLogMapper();
		accessLogMsgObj.setRawMessage(rawMsg);
		accessLogMsgObj.getKafkaMetaData().setOffset(offset);
		accessLogMsgObj.getKafkaMetaData().setTopic(this.getConfig().topic);
		accessLogMsgObj.getKafkaMetaData().setConsumerGroupName(this.getConfig().consumerGroupName);
		accessLogMsgObj.getKafkaMetaData().setPartition(this.getConfig().partition);
		
		accessLogMsgObj.setIp(splittedMsg[0].trim());
		accessLogMsgObj.setProtocol(splittedMsg[1].trim());
		
		
		if(splittedMsg[2].trim().toUpperCase().contains("GET")){
			accessLogMsgObj.setMethod(splittedMsg[2].trim());
			accessLogMsgObj.setPayLoad(splittedMsg[3].trim());
			accessLogMsgObj.setResponseCode(Integer.parseInt(splittedMsg[7].trim()));
			accessLogMsgObj.setSessionID(splittedMsg[8].trim());
			String[] serverAndInstance = splittedMsg[8].trim().split("\\.")[1].split("-");
			accessLogMsgObj.setServerAndInstance(splittedMsg[8].trim().split("\\.")[1]);

			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setHostName(splittedMsg[13].trim());
			accessLogMsgObj.setResponseTime(Integer.parseInt(splittedMsg[14].trim()));
			accessLogMsgObj.setUrl(splittedMsg[12].trim());
			
			SimpleDateFormat actualFormat = new SimpleDateFormat(actualDateFormat);
			actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			SimpleDateFormat expectedFormat = new SimpleDateFormat(expectedDateFormat);
			expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			Date date = actualFormat.parse(splittedMsg[9].trim().replaceAll("\\[", ""));
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));
		}
		
		
		if(splittedMsg[2].trim().toUpperCase().contains("POST")){
			accessLogMsgObj.setMethod(splittedMsg[2].trim());
			accessLogMsgObj.setPayLoad(null);
			accessLogMsgObj.setResponseCode(Integer.parseInt(splittedMsg[7].trim()));
			accessLogMsgObj.setSessionID(splittedMsg[8].trim());
			accessLogMsgObj.setServerAndInstance(splittedMsg[8].trim().split("\\.")[1]);
			String[] serverAndInstance = splittedMsg[8].trim().split("\\.")[1].split("-");
			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setHostName(splittedMsg[13].trim());
			accessLogMsgObj.setResponseTime(Integer.parseInt(splittedMsg[14].trim()));
			accessLogMsgObj.setUrl(splittedMsg[12].trim());
			
			SimpleDateFormat actualFormat = new SimpleDateFormat(actualDateFormat);
			actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			SimpleDateFormat expectedFormat = new SimpleDateFormat(expectedDateFormat);
			expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			Date date = actualFormat.parse(splittedMsg[9].trim().replaceAll("\\[", ""));
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));		
		}
			
	return mapper.writeValueAsString(accessLogMsgObj);
		
	}

}
