package org.elasticsearch.kafka.consumer;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class ConsumerJob {

    private final ConsumerConfig consumerConfig;
    private long readOffset;
    private MessageHandler msgHandler;
    private Client esClient;
    private KafkaClient kafkaConsumerClient;
    private Long currentOffsetAtProcess;
    private Long nextOffsetToProcess;
    //StatsReporter statsd;
    //private long statsLastPrintTime;
    //private Stats stats = new Stats();
    private boolean isStartingFirstTime;
    private String consumerGroupTopicPartition;
    Logger logger = ConsumerLogger.getLogger(this.getClass());

    public ConsumerJob(final ConsumerConfig config) throws Exception {
        consumerConfig = config;
        isStartingFirstTime = true;
        initKafka();
        initElasticSearch();
        createMessageHandler();
    }

    void initElasticSearch() throws Exception {
        logger.info("Initializing ElasticSearch");
        logger.info("esClusterName is::" + consumerConfig.getEsClusterName());
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", consumerConfig.getEsClusterName()).build();
        try {
            esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(consumerConfig.getEsHost(), consumerConfig.getEsPort()));
            logger.info("Initializing ElasticSearch Success. ElasticSearch Client created and initialized.");
        } catch (final Exception e) {
            logger.fatal("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::" + e.getMessage());
            throw e;
        }

    }

    void initKafka() throws Exception {
        logger.info("Initializing Kafka");
        String consumerGroupName = consumerConfig.getConsumerGroupName();
        if (consumerGroupName.isEmpty()) {
            consumerGroupName = "Client_" + consumerConfig.getTopic() + "_" + consumerConfig.getPartition();
            logger.info("ConsumerGroupName is empty.Hence created a group name");
        }
        logger.info("consumerGroupName is:" + consumerGroupName);
        consumerGroupTopicPartition = consumerGroupName + "_" + consumerConfig.getTopic() + "_" + consumerConfig.getPartition();
        logger.info("consumerGroupTopicPartition is:" + consumerGroupTopicPartition);
        kafkaConsumerClient = new KafkaClient(consumerConfig, consumerConfig.getZookeeper(), consumerConfig.getBrokerHost(), consumerConfig.getBrokerPort(), consumerConfig.getPartition(), consumerGroupName, consumerConfig.getTopic());
        logger.info("Kafka intialization success and kafka client created and initialized");
    }


    void reInitKafka() throws Exception {
        logger.info("Kafka Reintialization Kafka & Consumer Client");
        kafkaConsumerClient.close();
        logger.info("Kafka client closed");
        kafkaConsumerClient.findNewLeader();
        logger.info("Found new leader in Kafka broker");
        kafkaConsumerClient.initConsumer();
        logger.info("Kafka Reintialization Kafka & Consumer Client is success");
    }

    public long computeOffset() throws Exception {
        if (!isStartingFirstTime) {
            logger.info("This is not 1st time read in Kafka");
            readOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
            logger.info("computedOffset:=" + readOffset);
            return readOffset;
        }
        logger.info("**** Starting the Kafka Read for 1st time ***");
        if (consumerConfig.getStartOffsetFrom().equalsIgnoreCase("CUSTOM")) {
            logger.info("startOffsetFrom is CUSTOM");
            logger.info("startOffset is given as:" + consumerConfig.getStartOffset());
            if (consumerConfig.getStartOffset() != -1) {
                readOffset = consumerConfig.getStartOffset();
            } else {
                logger.fatal("Custom start from Offset for" + consumerGroupTopicPartition + " is -1 which is a not acceptable value. When startOffsetFrom config value is custom, then a valid startOffset has to be provided. Exiting !");
                throw new RuntimeException("Custom start from Offset for" + consumerGroupTopicPartition + " is -1 which is a not acceptable value. When startOffsetFrom config value is custom, then a valid startOffset has to be provided");
            }
        } else if (consumerConfig.getStartOffsetFrom().equalsIgnoreCase("OLDEST")) {
            logger.info("startOffsetFrom is selected as OLDEST");
            readOffset = kafkaConsumerClient.getOldestOffset();
        } else if (consumerConfig.getStartOffsetFrom().equalsIgnoreCase("LATEST")) {
            logger.info("startOffsetFrom is selected as LATEST");
            readOffset = kafkaConsumerClient.getLatestOffset();
        } else if (consumerConfig.getStartOffsetFrom().equalsIgnoreCase("RESTART")) {
            logger.info("ReStarting from where the Offset is left for consumer:" + consumerGroupTopicPartition);
            logger.info("startOffsetFrom is selected as RESTART");
            readOffset = kafkaConsumerClient.fetchCurrentOffsetFromKafka();
        }
        isStartingFirstTime = false;
        logger.info("computedOffset:=" + readOffset);
        //System.out.println("readOffset:=" + readOffset);
        return readOffset;
    }


    private void createMessageHandler() throws Exception {
        try {
            logger.info("MessageHandler Class given in config is:" + consumerConfig.getMessageHandlerClass());
            msgHandler = (MessageHandler) Class.forName(consumerConfig.getMessageHandlerClass()).newInstance();
            msgHandler.initMessageHandler(esClient, consumerConfig);
            logger.info("Created an initialized MessageHandle::" + consumerConfig.getMessageHandlerClass());
        } catch (final Exception e) {
            e.printStackTrace();
            throw e;

        }
    }

    public void doRun() throws Exception {
        logger.info("***** Starting a new round of kafka read and post to ElasticSearch ******");
        final long jobStartTime = System.currentTimeMillis();
        final LinkedHashMap<Long, Message> offsetMsgMap = new LinkedHashMap<>();
        computeOffset();
        final FetchResponse fetchResponse = kafkaConsumerClient.getFetchResponse(readOffset, consumerConfig.getBulkSize());
        final long timeAfterKafkaFetch = System.currentTimeMillis();
        logger.info("Fetched the response from Kafka. Approx time taken is::" + (timeAfterKafkaFetch - jobStartTime) + " milliSec");
        if (fetchResponse.hasError()) {
            //Do things according to the error code
            final short errorCode = fetchResponse.errorCode(consumerConfig.getTopic(), consumerConfig.getPartition());
            logger.error("Kafka fetch error happened. Error code is::" + errorCode + " Starting to handle the error");
            if (errorCode == ErrorMapping.BrokerNotAvailableCode()) {
                logger.error("BrokerNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
                reInitKafka();
                return;

            } else if (errorCode == ErrorMapping.InvalidFetchSizeCode()) {
                logger.error("InvalidFetchSizeCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
                reInitKafka();
                return;

            } else if (errorCode == ErrorMapping.InvalidMessageCode()) {
                logger.error("InvalidMessageCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
                logger.error("LeaderNotAvailableCode error happened when fetching message from Kafka. ReInitiating Kafka Client");
                reInitKafka();
                return;

            } else if (errorCode == ErrorMapping.MessageSizeTooLargeCode()) {
                logger.error("MessageSizeTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.NotLeaderForPartitionCode()) {
                logger.error("NotLeaderForPartitionCode error happened when fetching message from Kafka, not handling it. ReInitiating Kafka Client.");
                reInitKafka();
                return;

            } else if (errorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                logger.error("OffsetMetadataTooLargeCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                logger.error("OffsetOutOfRangeCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.ReplicaNotAvailableCode()) {
                logger.error("ReplicaNotAvailableCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.RequestTimedOutCode()) {
                logger.error("RequestTimedOutCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.StaleControllerEpochCode()) {
                logger.error("StaleControllerEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.StaleLeaderEpochCode()) {
                logger.error("StaleLeaderEpochCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.UnknownCode()) {
                logger.error("UnknownCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            } else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
                logger.error("UnknownTopicOrPartitionCode error happened when fetching message from Kafka, not handling it. Returning.");
                return;

            }
            return;
        }
        final ByteBufferMessageSet byteBufferMsgSet = kafkaConsumerClient.fetchMessageSet(fetchResponse);
        final long timeAftKafaFetchMsgSet = System.currentTimeMillis();
        logger.info("Completed MsgSet fetch from Kafka. Approx time taken is::" + (timeAftKafaFetchMsgSet - timeAfterKafkaFetch) + " milliSec");
        if (byteBufferMsgSet.validBytes() <= 0) {
            logger.warn("No valid bytes available in Kafka response MessageSet. Sleeping for 1 second");
            Thread.sleep(1000);
            return;
        }
        final Iterator<MessageAndOffset> msgOffSetIter = byteBufferMsgSet.iterator();
        while (msgOffSetIter.hasNext()) {
            final MessageAndOffset msgAndOffset = msgOffSetIter.next();
            currentOffsetAtProcess = msgAndOffset.offset();
            nextOffsetToProcess = msgAndOffset.nextOffset();
            offsetMsgMap.put(currentOffsetAtProcess, msgAndOffset.message());
        }
        final long timeAftKafkaMsgCollate = System.currentTimeMillis();
        logger.info("Completed collating the Messages and Offset into Map. Approx time taken is::" + (timeAftKafkaMsgCollate - timeAftKafaFetchMsgSet) + " milliSec");
        //this.msgHandler.initMessageHandler();
        //logger.info("Initialized Message handler");
        msgHandler.setOffsetMsgMap(offsetMsgMap);
        logger.info("Starting to transform the messages");
        msgHandler.transformMessage();
        logger.info("Completed transforming messages");
        logger.info("Starting to prepare ElasticSearch");
        msgHandler.prepareForPostToElasticSearch();
        final long timeAtPrepareES = System.currentTimeMillis();
        logger.info("Completed preparing ElasticSearch.Approx time taken to initMsg,TransformMsg,Prepare ES is::" + (timeAtPrepareES - timeAftKafkaMsgCollate) + " milliSec");
        logger.info("nextOffsetToProcess is::" + nextOffsetToProcess + "This is the offset that will be commited once elasticSearch post is complete");
        final boolean esPostResult = msgHandler.postToElasticSearch();
        final long timeAftEsPost = System.currentTimeMillis();
        logger.info("Approx time it took to post of ElasticSearch is:" + (timeAftEsPost - timeAtPrepareES) + " milliSec");
        if (esPostResult) {
            logger.info("Commiting offset #::" + nextOffsetToProcess);
            kafkaConsumerClient.saveOffsetInKafka(nextOffsetToProcess, fetchResponse.errorCode(consumerConfig.getTopic(), consumerConfig.getPartition()));
        } else {
            logger.error("Posting to ElasticSearch has error");
            //Need to decide about what to do when message failure rate is > set messageFailuresTolerance. For now still commiting the
        }
        final long timeAtEndOfJob = System.currentTimeMillis();
        logger.info("*** This round of ConsumerJob took approx:: " + (timeAtEndOfJob - jobStartTime) + " milliSec." + "Messages from Offset:" + readOffset + " to " + currentOffsetAtProcess + " were processed in this round. ****");
    }


    public void stop() {
        logger.info("About to close the Kafka & ElasticSearch Client");
        kafkaConsumerClient.close();
        esClient.close();
        logger.info("Closed Kafka & ElasticSearch Client");
    }

}


