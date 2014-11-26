package org.elasticsearch.kafka.consumer.messageHandlers;

import kafka.consumer.ConsumerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.mappers.AccessLogMapper;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class AccessLogMessageHandler extends RawMessageStringHandler {
	
	
	/* This Message Handler class is an Example to show how Messahe Handler Class can be implemented
	 * This class modifies each line in the Access Log(from Apache WebServer) to desired JSON message as defined by the org.elasticsearch.kafka.consumer.mappers.AccessLogMapper class
	 */
	
	
	private final static String actualDateFormat = "dd/MMM/yyyy:hh:mm:ss";
	private final static String expectedDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private final static String actualTimeZone = "Europe/London";
	private final static String expectedTimeZone = "Europe/London";
	
	
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
		String[] splitMsg=  rawMsg.split(" ");

		AccessLogMapper accessLogMsgObj = new AccessLogMapper();
		accessLogMsgObj.setRawMessage(rawMsg);
		accessLogMsgObj.getKafkaMetaData().setOffset(offset);
		accessLogMsgObj.getKafkaMetaData().setTopic(this.getConfig().getTopic());
		accessLogMsgObj.getKafkaMetaData().setConsumerGroupName(this.getConfig().getConsumerGroupName());
		accessLogMsgObj.getKafkaMetaData().setPartition(this.getConfig().getPartition());
		
		accessLogMsgObj.setIp(splitMsg[0].trim());
		accessLogMsgObj.setProtocol(splitMsg[1].trim());
		
		
		if(splitMsg[2].trim().toUpperCase().contains("GET")){
			accessLogMsgObj.setMethod(splitMsg[2].trim());
			accessLogMsgObj.setPayLoad(splitMsg[3].trim());
			accessLogMsgObj.setResponseCode(Integer.parseInt(splitMsg[7].trim()));
			accessLogMsgObj.setSessionID(splitMsg[8].trim());
			String[] serverAndInstance = splitMsg[8].trim().split("\\.")[1].split("-");
			accessLogMsgObj.setServerAndInstance(splitMsg[8].trim().split("\\.")[1]);

			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setHostName(splitMsg[13].trim());
			accessLogMsgObj.setResponseTime(Integer.parseInt(splitMsg[14].trim()));
			accessLogMsgObj.setUrl(splitMsg[12].trim());
			
			SimpleDateFormat actualFormat = new SimpleDateFormat(actualDateFormat);
			actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			SimpleDateFormat expectedFormat = new SimpleDateFormat(expectedDateFormat);
			expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			Date date = actualFormat.parse(splitMsg[9].trim().replaceAll("\\[", ""));
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));
		}
		
		
		if(splitMsg[2].trim().toUpperCase().contains("POST")){
			accessLogMsgObj.setMethod(splitMsg[2].trim());
			accessLogMsgObj.setPayLoad(null);
			accessLogMsgObj.setResponseCode(Integer.parseInt(splitMsg[7].trim()));
			accessLogMsgObj.setSessionID(splitMsg[8].trim());
			accessLogMsgObj.setServerAndInstance(splitMsg[8].trim().split("\\.")[1]);
			String[] serverAndInstance = splitMsg[8].trim().split("\\.")[1].split("-");
			accessLogMsgObj.setServerName(serverAndInstance[0].trim());
			accessLogMsgObj.setInstance(serverAndInstance[1].trim());
			accessLogMsgObj.setHostName(splitMsg[13].trim());
			accessLogMsgObj.setResponseTime(Integer.parseInt(splitMsg[14].trim()));
			accessLogMsgObj.setUrl(splitMsg[12].trim());
			
			SimpleDateFormat actualFormat = new SimpleDateFormat(actualDateFormat);
			actualFormat.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
			
			SimpleDateFormat expectedFormat = new SimpleDateFormat(expectedDateFormat);
			expectedFormat.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
			
			Date date = actualFormat.parse(splitMsg[9].trim().replaceAll("\\[", ""));
			accessLogMsgObj.setTimeStamp(expectedFormat.format(date));		
		}
			
	return mapper.writeValueAsString(accessLogMsgObj);
		
	}

}
