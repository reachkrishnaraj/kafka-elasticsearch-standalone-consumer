package org.elasticsearch.kafka.indexer.messageHandlers;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.kafka.indexer.ConsumerConfig;
import org.elasticsearch.kafka.indexer.MessageHandler;
import org.elasticsearch.kafka.indexer.mappers.AccessLogMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccessLogMessageHandler extends MessageHandler {
	
	private static final Logger logger = LoggerFactory.getLogger(AccessLogMessageHandler.class);
	private final String actualDateFormat = "dd/MMM/yyyy:hh:mm:ss";
	private final String expectedDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private final String actualTimeZone = "Europe/London";
	private final String expectedTimeZone = "Europe/London";
	
	private ObjectMapper mapper = new ObjectMapper();
	private AccessLogMapper accessLogMsgObj = new AccessLogMapper();
	
	private String[] splittedMsg = null;
	private SimpleDateFormat actualFormat = null;
	private SimpleDateFormat expectedFormat = null;
	private String dateString[] = null;
	private Date date = null;
	private String[] serverAndInstance = null;
	
	public AccessLogMessageHandler(TransportClient client,ConsumerConfig config) throws Exception{
		super(client, config);
		logger.info("Initialized org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler");		
	}
	
	@Override
	public byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception{
		String outputMessageStr = this.convertToJson(new String(inputMessage, "UTF-8"), offset);
		return outputMessageStr.getBytes(); 		
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
		accessLogMsgObj.getKafkaMetaData().setPartition(this.getConfig().firstPartition);		
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
