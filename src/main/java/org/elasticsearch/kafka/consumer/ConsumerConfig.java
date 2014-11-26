package org.elasticsearch.kafka.consumer;

import org.apache.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;

import java.io.InputStream;
import java.util.Properties;

public class ConsumerConfig {

    //Logger object cannot be initialized since the logProperty file for the instance would be known only after config is read
    //Logger logger = ConsumerLogger.initLogger(this.getClass());
    private final int BULK_MSG_SIZE = 10 * 1024 * 1024 * 3;
    private final int BULK_MSG_TIMEOUT = 10000;
    private final String BULK_MSG_TIMEOUT_STRING = "10ms";

    //Kafka ZooKeeper's IP Address/HostName without port
    private String zookeeper;
    //Full class path and name for the concrete message handler class factory
    private String messageHandlerClass;
    //Kafka Broker's IP Address/HostName
    private String brokerHost;
    //Kafka Broker's Port number
    private int brokerPort;
    //Kafka Topic from which the message has to be processed
    private String topic;
    //Partition in the Kafka Topic from which the message has to be processed
    private short partition;
    //Option from where the message fetching should happen in Kafka
    // Values can be: CUSTOM/OLDEST/LATEST/RESTART.
    // If 'CUSTOM' is set, then 'startOffset' has to be set an int value
    private String startOffsetFrom;
    //int value of the offset from where the message processing should happen
    private int startOffset;
    //Name of the Kafka Consumer Group
    private String consumerGroupName;
    private String statsdPrefix;
    private String statsdHost;
    private int statsdPort;
    //Preferred Size of message to be fetched from Kafka in 1 Fetch call to kafka
    private int bulkSize;
    //Timeout when fetching message from Kafka
    private TimeValue bulkTimeout;
    //Preferred Message Encoding to process the message before posting it to ElasticSearch
    private String messageEncoding;
    //TBD
    private boolean isGuranteedEsPostMode;
    //Name of the ElasticSearch Cluster
    private String esClusterName;
    //Hostname/ipAddress of ElasticSearch
    private String esHost;
    //Port number of ElasticSearch
    private int esPort;
    //IndexName in ElasticSearch to which the processed Message has to be posted
    private String esIndex;
    //IndexType in ElasticSearch to which the processed Message has to be posted
    private String esIndexType;
    //Percentage of message failure tolerance
    private int esMsgFailureTolerancePercent;

    //Log property file for the consumer instance
    private String logPropertyFile;

    private final Logger logger = ConsumerLogger.getLogger(this.getClass());

    public ConsumerConfig(final Properties properties) throws Exception {
        init(properties);
    }

    public ConsumerConfig(final String configFile) throws Exception {
        InputStream input;
        try {
            //logger.info("configFile Passed::"+configFile);
            input = this.getClass().getClassLoader().getResourceAsStream(configFile);
            logger.info("configFile loaded Successfully");
            //System.out.println("configFile loaded Successfully");
        } catch (final Exception e) {
            logger.fatal("Error reading/loading ConfigFile. Throwing the error. Error Message::" + e.getMessage());
            //System.out.println("Error reading/loading ConfigFile. Throwing the error. Error Message::" + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        Properties prop = null;
        if (input != null) {
            //System.out.println("configFile NOT loaded Successfully.Hence reading the default values for the properties");
            // load the properties file
            prop = new Properties();
            prop.load(input);
            input.close();
        }
        init(prop);
        //System.out.println("Config reading complete !");
        logger.info("configFile loaded,read and closed Successfully");
    }

    private void init(final Properties prop) {
        if (prop != null) {
            zookeeper = prop.getProperty("zookeeper", "localhost");
            messageHandlerClass = prop.getProperty("messageHandlerClass", "org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler");
            brokerHost = prop.getProperty("brokerHost", "localhost");
            brokerPort = Integer.parseInt(prop.getProperty("brokerPort", "9092"));
            topic = prop.getProperty("topic", "");
            partition = Short.parseShort(prop.getProperty("partition", "0"));
            startOffsetFrom = prop.getProperty("startOffsetFrom", "");
            startOffset = Integer.parseInt(prop.getProperty("startOffset", "0"));
            consumerGroupName = prop.getProperty("consumerGroupName", "ESKafkaConsumerClient");
            statsdPrefix = prop.getProperty("statsdPrefix", "");
            statsdHost = prop.getProperty("statsdHost", "");
            statsdPort = Integer.parseInt(prop.getProperty("statsdPort", "0"));
            bulkSize = Integer.parseInt(prop.getProperty("bulkSize",
                    String.valueOf(BULK_MSG_SIZE)));
            bulkTimeout = TimeValue.parseTimeValue(
                    (prop.getProperty("bulkTimeout", BULK_MSG_TIMEOUT_STRING)),
                    TimeValue.timeValueMillis(BULK_MSG_TIMEOUT));
            messageEncoding = prop.getProperty("messageEncoding", "UTF-8");
            isGuranteedEsPostMode = Boolean.getBoolean(prop.getProperty("isGuranteedEsPostMode", "false"));

            esClusterName = prop.getProperty("esClusterName", "");
            esHost = prop.getProperty("esHost", "localhost");
            esPort = Integer.parseInt(prop.getProperty("esPort", "9300"));
            esIndex = prop.getProperty("esIndex", "kafkaConsumerIndex");
            esIndexType = prop.getProperty("esIndexType", "kafka");
            esMsgFailureTolerancePercent = Integer.parseInt(prop.getProperty("esMsgFailureTolerancePercent", "5"));
            logPropertyFile = prop.getProperty("logPropertyFile", "log4j.properties");
        } else {
            zookeeper = "localhost";
            messageHandlerClass = "";
            brokerHost = "";
            brokerPort = 0;
            topic = "";
            partition = 0;
            startOffsetFrom = "";
            startOffset = 0;
            consumerGroupName = "";
            statsdPrefix = "";
            statsdHost = "";
            statsdPort = 0;
            bulkSize = BULK_MSG_SIZE;
            bulkTimeout = TimeValue.timeValueMillis(BULK_MSG_TIMEOUT);
            messageEncoding = "UTF-8";
            isGuranteedEsPostMode = false;
            esClusterName = "elasticsearch";
            esHost = "localhost";
            esPort = 9200;
            esIndex = "kafkaConsumerIndex";
            esIndexType = "kafka";
            esMsgFailureTolerancePercent = 5;
            logPropertyFile = "log4j.properties";
        }
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public String getMessageHandlerClass() {
        return messageHandlerClass;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public String getTopic() {
        return topic;
    }

    public short getPartition() {
        return partition;
    }

    public String getStartOffsetFrom() {
        return startOffsetFrom;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public String getStatsdPrefix() {
        return statsdPrefix;
    }

    public String getStatsdHost() {
        return statsdHost;
    }

    public int getStatsdPort() {
        return statsdPort;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public TimeValue getBulkTimeout() {
        return bulkTimeout;
    }

    public String getMessageEncoding() {
        return messageEncoding;
    }

    public boolean isGuranteedEsPostMode() {
        return isGuranteedEsPostMode;
    }

    public String getEsClusterName() {
        return esClusterName;
    }

    public String getEsHost() {
        return esHost;
    }

    public int getEsPort() {
        return esPort;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public String getEsIndexType() {
        return esIndexType;
    }

    public int getEsMsgFailureTolerancePercent() {
        return esMsgFailureTolerancePercent;
    }

    public String getLogPropertyFile() {
        return logPropertyFile;
    }
}
