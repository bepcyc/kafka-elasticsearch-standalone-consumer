package org.elasticsearch.kafka.consumer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.Properties;

public class ConsumerLogger {

	public static Logger logger;
	
	public static void doInitLogger(ConsumerConfig config) throws IOException{
		Properties logProp = new Properties();		
		System.out.println("logPropertyFile::" + config.logPropertyFile);
		System.out.println("logPropFileInStr::" + ConsumerLogger.class.getClassLoader().getResourceAsStream(config.logPropertyFile));
		logProp.load(ConsumerLogger.class.getClassLoader().getResourceAsStream(config.logPropertyFile));
		PropertyConfigurator.configure(logProp);
		//PropertyConfigurator.configure(config.logPropertyFile);
	}
	
	public static Logger getLogger(Class<?> cls){		
		return Logger.getLogger(cls);
	}
	
	
	
	//public static Logger logger = Logger.getLogger(ConsumerLogger.class);
	
}
