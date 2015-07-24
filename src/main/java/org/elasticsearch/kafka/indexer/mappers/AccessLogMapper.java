package org.elasticsearch.kafka.indexer.mappers;

public class AccessLogMapper {

	private KafkaMetaDataMapper kafkaMetaData = new KafkaMetaDataMapper();
	private String ip;
	private String protocol;
	private String method;
	private String url;
	private String payLoad;
	private String sessionID;
	private String timeStamp;
	private Integer responseTime;
	private Integer responseCode;
	private String hostName;
	private String serverName;
	private String serverAndInstance;
	private String instance;
	private String sourceIpAndPort;
	private String ajpThreadName;
	private String rawMessage;

	public KafkaMetaDataMapper getKafkaMetaData() {
		return kafkaMetaData;
	}

	public void setKafkaMetaData(KafkaMetaDataMapper kafkaMetaData) {
		this.kafkaMetaData = kafkaMetaData;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getPayLoad() {
		return payLoad;
	}

	public void setPayLoad(String payLoad) {
		this.payLoad = payLoad;
	}

	public String getSessionID() {
		return sessionID;
	}

	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Integer getResponseTime() {
		return responseTime;
	}

	public void setResponseTime(Integer responseTime) {
		this.responseTime = responseTime;
	}

	public Integer getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(Integer responseCode) {
		this.responseCode = responseCode;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public String getSourceIpAndPort() {
		return sourceIpAndPort;
	}

	public void setSourceIpAndPort(String sourceIpAndPort) {
		this.sourceIpAndPort = sourceIpAndPort;
	}

	public String getServerAndInstance() {
		return serverAndInstance;
	}

	public void setServerAndInstance(String serverAndInstance) {
		this.serverAndInstance = serverAndInstance;
	}

	public String getRawMessage() {
		return rawMessage;
	}

	public void setRawMessage(String rawMessage) {
		this.rawMessage = rawMessage;
	}

	public String getAjpThreadName() {
		return ajpThreadName;
	}

	public void setAjpThreadName(String ajpThreadName) {
		this.ajpThreadName = ajpThreadName;
	}

	
	
}
