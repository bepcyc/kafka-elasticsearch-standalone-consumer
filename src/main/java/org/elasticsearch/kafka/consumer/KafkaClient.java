package org.elasticsearch.kafka.consumer;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClient {


    private final static int FIND_KAFKA_LEADER_TIMEOUT = 10000;
    private CuratorFramework curator;
    private SimpleConsumer simpleConsumer;
    private final String zooKeeper;
    private final String incomingBrokerHost;
    private final int incomingBrokerPort;
    private final String incomingBrokerURL;
    private final String clientName;
    private final String topic;
    private final int partition;
    private final List<String> replicaBrokers;
    private String leadBrokerHost;
    private int leadBrokerPort;
    private String leadBrokerURL;
    private final ConsumerConfig consumerConfig;
    private final Logger logger = ConsumerLogger.getLogger(this.getClass());


    public KafkaClient(final ConsumerConfig config, final String zooKeeper, final String brokerHost, final int brokerPort, final int partition, final String clientName, final String topic) throws Exception {
        logger.info("Instantiating KafkaClient");
        consumerConfig = config;

        this.zooKeeper = zooKeeper;
        this.topic = topic;
        incomingBrokerHost = brokerHost;
        incomingBrokerPort = brokerPort;
        incomingBrokerURL = incomingBrokerHost + ":" + incomingBrokerPort;
        this.clientName = clientName;
        this.partition = partition;
        logger.info("### Printing out Config Value passed ###");
        logger.info("zooKeeper::" + this.zooKeeper);
        logger.info("topic::" + this.topic);
        logger.info("incomingBrokerHost::" + incomingBrokerHost);
        logger.info("incomingBrokerPort::" + incomingBrokerPort);
        logger.info("inComingBrokerURL::" + incomingBrokerURL);
        logger.info("clientName::" + this.clientName);
        logger.info("partition::" + this.partition);
        replicaBrokers = new ArrayList<>();
        logger.info("Starting to connect to Zookeeper");
        connectToZooKeeper(this.zooKeeper);
        logger.info("Starting to find the Kafka Lead Broker");
        findLeader();
        logger.info("Starting to initiate Kafka Consumer");
        initConsumer();

    }

    void connectToZooKeeper(final String zooKeeper) throws Exception {
        try {
            curator = CuratorFrameworkFactory.newClient(zooKeeper, 1000, 15000,
                    new RetryNTimes(5, 2000));
            curator.start();
            logger.info("Connection to Kafka Zookeeper successfull");
        } catch (final Exception e) {
            logger.fatal("Failed to connect to Zookeer. Throwing the exception. Error message is::" + e.getMessage());
            throw e;
        }
    }

    public void initConsumer() throws Exception {
        try {
            simpleConsumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort, 5000, 1024 * 1024 * 10, clientName);
            logger.info("Successfully initialized Kafka Consumer");
        } catch (final Exception e) {
            logger.fatal("Failed to initialize Kafka Consumer. Throwing the Error. Error Message is::" + e.getMessage());
            throw e;
        }
    }

    //This method is NOT USED. Revisit to delete
    public void save(final String path, final String data) throws Exception {
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, data.getBytes());
            } else {
                curator.setData().forPath(path, data.getBytes());
            }
        } catch (final Exception e) {
            //logger.error("Error trying to save" + e.getMessage());
            throw e;
        }
    }

    //This method is NOT USED. Revisit to delete
    public String get(final String path) {
        try {
            if (curator.checkExists().forPath(path) != null) {
                return new String(curator.getData().forPath(path));
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    //This method is NOT USED. Revisit to delete
    public void saveOffset(final String topic, final int partition, final long offset) throws Exception {
        save(String.format("/es-river-kafka/offsets/%s/%s/%d",
                leadBrokerURL, topic, partition), Long.toString(offset));
    }


    public short saveOffsetInKafka(final long offset, final short errorCode) throws Exception {
        logger.info("Starting to save the Offset value to Kafka. Offset value is::" + offset + " and errorCode passed is::" + errorCode);
        final short versionID = 0;
        final int correlationId = 0;
        try {
            final TopicAndPartition tp = new TopicAndPartition(topic, partition);
            final OffsetMetadataAndError offsetMetaAndErr = new OffsetMetadataAndError(offset, OffsetMetadataAndError.NoMetadata(), errorCode);
            final Map<TopicAndPartition, OffsetMetadataAndError> mapForCommitOffset = new HashMap<>();
            mapForCommitOffset.put(tp, offsetMetaAndErr);
            final kafka.javaapi.OffsetCommitRequest offsetCommitReq = new kafka.javaapi.OffsetCommitRequest(clientName, mapForCommitOffset, versionID, correlationId, clientName);
            final OffsetCommitResponse offsetCommitResp = simpleConsumer.commitOffsets(offsetCommitReq);
            logger.info("Completed OffsetSet commit. OffsetCommitResponse ErrorCode is::" + offsetCommitResp.errors().get(tp) + "Returning to the caller.");
            return (Short) offsetCommitResp.errors().get(tp);
        } catch (final Exception e) {
            logger.fatal("Error when committing Offset to Kafka. Throwing the error. Error Message is::" + e.getMessage());
            throw e;
        }
    }

    public long fetchCurrentOffsetFromKafka() throws Exception {
        logger.info("Starting to fetchCurrentOffsetFromKafka");
        final short versionID = 0;
        final int correlationId = 0;
        try {
            final List<TopicAndPartition> tp = new ArrayList<>();
            tp.add(new TopicAndPartition(topic, partition));
            final OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(clientName, tp, versionID, correlationId, clientName);
            final OffsetFetchResponse response = simpleConsumer.fetchOffsets(offsetFetchReq);
            logger.info("Completed fetchCurrentOffsetFromKafka. CurrentOffset Fetched is::" + response.offsets().get(tp.get(0)).offset());
            return response.offsets().get(tp.get(0)).offset();
        } catch (final Exception e) {
            logger.fatal("Error when fetching current offset from kafka. Throwing the exception. Error Message is::" + e.getMessage());
            throw e;
        }
    }

    private PartitionMetadata findLeader() throws Exception {
        logger.info("Starting to find the leader broker for Kafka");
        PartitionMetadata returnMetaData = null;
        SimpleConsumer leadFindConsumer = null;
        try {
            leadFindConsumer = new SimpleConsumer(incomingBrokerHost, incomingBrokerPort, FIND_KAFKA_LEADER_TIMEOUT,
                    consumerConfig.getBulkSize(), "leaderLookup");
            final List<String> topics = new ArrayList<>();
            topics.add(topic);
            final TopicMetadataRequest req = new TopicMetadataRequest(topics);
            final kafka.javaapi.TopicMetadataResponse resp = leadFindConsumer.send(req);

            final List<TopicMetadata> metaData = resp.topicsMetadata();
            for (final TopicMetadata item : metaData) {
                for (final PartitionMetadata part : item.partitionsMetadata()) {
                    if (part.partitionId() == partition) {
                        System.out.println("ITS TRUE");
                        returnMetaData = part;
                        break;
                    }
                }
            }
        } catch (final Exception e) {
            logger.fatal("Error communicating with Broker [" + incomingBrokerHost
                    + "] to find Leader for [" + topic + ", " + partition
                    + "] Reason: " + e.getMessage());
            throw e;
        } finally {
            if (leadFindConsumer != null)
                leadFindConsumer.close();
            logger.info("closed the connection");

        }
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (final kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }

        leadBrokerHost = returnMetaData.leader().host();
        leadBrokerPort = returnMetaData.leader().port();
        leadBrokerURL = leadBrokerHost + ":" + leadBrokerPort;
        logger.info("Computed leadBrokerURL is::" + leadBrokerURL + ", Returning.");
        return returnMetaData;
    }


    public PartitionMetadata findNewLeader() throws Exception {
        logger.info("Starting to findNewLeader for the kafka");
        for (int i = 0; i < 3; i++) {
            logger.info("Attempt # ::" + i);
            boolean goToSleep = false;
            final PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                logger.info("MetaData is Empty, going to sleep");
                goToSleep = true;
            } else if (metadata.leader() == null) {
                logger.info("MetaData leader is Empty, going to sleep");
                goToSleep = true;
            } else if (leadBrokerHost.equalsIgnoreCase(metadata.leader().host()) && (i <= 1)) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                leadBrokerHost = metadata.leader().host();
                leadBrokerPort = metadata.leader().port();
                leadBrokerURL = leadBrokerHost + ":" + leadBrokerPort;
                logger.info("Found and Computed leadBrokerURL is::" + leadBrokerURL + ", returning");
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ie) {
                }
            }
        }
        logger.fatal("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }


    public long getLatestOffset() throws Exception {
        Long latestOffset;
        logger.info("Trying to get the LatestOffset for the topic: " + clientName);
        try {
            latestOffset = getOffset(topic, partition, OffsetRequest.LatestTime(), clientName);
        } catch (final Exception e) {
            logger.fatal("Exception when trying to get the getLatestOffset. Throwing the exception. Error is:" + e.getMessage());
            throw e;
        }
        logger.info("LatestOffset is::" + latestOffset);
        return latestOffset;

    }

    public long getOldestOffset() throws Exception {
        Long oldestOffset;
        logger.info("Trying to get the OldestOffset for the topic: " + clientName);
        try {
            oldestOffset = getOffset(topic, partition, OffsetRequest.EarliestTime(), clientName);
        } catch (final Exception e) {
            logger.fatal("Exception when trying to get the getOldestOffset. Throwing the exception. Error is:" + e.getMessage());
            throw e;
        }
        logger.info("oldestOffset is::" + oldestOffset);
        return oldestOffset;
    }

    private long getOffset(final String topic, final int partition, final long whichTime, final String clientName) throws Exception {
        logger.info("Starting the generic getOffset");
        try {
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                    partition);
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    whichTime, 1));
            final kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                    clientName);
            final OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

            if (response.hasError()) {
                logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
                throw new Exception("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            }
            final long[] offsets = response.offsets(topic, partition);
            logger.info("Offset is::" + offsets[0]);
            return offsets[0];

        } catch (final Exception e) {
            logger.fatal("Exception when trying to get the Offset. Throwing the exception. Error is:" + e.getMessage());
            throw e;
        }
    }

    FetchResponse getFetchResponse(final long offset, final int maxSizeBytes) throws Exception {
        logger.info("Starting getFetchResponse");
        try {
            final FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, offset, maxSizeBytes).build();
            final FetchResponse fetchResponse = simpleConsumer.fetch(req);
            logger.info("Fetch from Kafka excecuted without Exception. Returning with the fetchResponse");
            return fetchResponse;
        } catch (final Exception e) {
            logger.fatal("Exception when trying to fetch the messages from Kafka. Throwing the exception. Error message is::" + e.getMessage());
            throw e;
        }


    }

    ByteBufferMessageSet fetchMessageSet(final FetchResponse fetchReponse) {
        return fetchReponse.messageSet(topic, partition);
    }

    public void close() {
        curator.close();
        logger.info("Curator/Zookeeper connection closed");
    }

}
