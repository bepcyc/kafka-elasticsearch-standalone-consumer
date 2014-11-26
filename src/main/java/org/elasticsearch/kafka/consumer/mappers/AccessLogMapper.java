package org.elasticsearch.kafka.consumer.mappers;

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
    private String serverAndInstance;
    private String serverName;
    private String instance;
    private String rawMessage;

    public KafkaMetaDataMapper getKafkaMetaData() {
        return kafkaMetaData;
    }

    public void setKafkaMetaData(final KafkaMetaDataMapper kafkaMetaData) {
        this.kafkaMetaData = kafkaMetaData;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(final String ip) {
        this.ip = ip;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(final String protocol) {
        this.protocol = protocol;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(final String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(final String payLoad) {
        this.payLoad = payLoad;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(final String sessionID) {
        this.sessionID = sessionID;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(final String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Integer getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(final Integer responseTime) {
        this.responseTime = responseTime;
    }

    public Integer getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(final Integer responseCode) {
        this.responseCode = responseCode;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(final String serverName) {
        this.serverName = serverName;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(final String instance) {
        this.instance = instance;
    }

    public String getServerAndInstance() {
        return serverAndInstance;
    }

    public void setServerAndInstance(final String serverAndInstance) {
        this.serverAndInstance = serverAndInstance;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(final String rawMessage) {
        this.rawMessage = rawMessage;
    }

}
