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

	
	public KafkaClient(ConsumerConfig config, String zooKeeper, String brokerHost, int brokerPort, int partition, String clientName, String topic) throws Exception {
		logger.info("Instantiating KafkaClient");
		this.consumerConfig = config;
		
		this.zooKeeper = zooKeeper;
		this.topic = topic;
		this.incomingBrokerHost = brokerHost;
		this.incomingBrokerPort = brokerPort;
		this.incomingBrokerURL = this.incomingBrokerHost + ":" + this.incomingBrokerPort;
		this.clientName = clientName;
		this.partition = partition;
		logger.info("### Printing out Config Value passed ###");
		logger.info("zooKeeper::" + this.zooKeeper);
		logger.info("topic::" + this.topic);
		logger.info("incomingBrokerHost::" + this.incomingBrokerHost);
		logger.info("incomingBrokerPort::" + this.incomingBrokerPort);
		logger.info("inComingBrokerURL::" + this.incomingBrokerURL);
		logger.info("clientName::" + this.clientName);
		logger.info("partition::" + this.partition);
		replicaBrokers = new ArrayList<String>();
		logger.info("Starting to connect to Zookeeper");
		connectToZooKeeper(this.zooKeeper);
		logger.info("Starting to find the Kafka Lead Broker");
		findLeader();
		logger.info("Starting to initiate Kafka Consumer");
		initConsumer();
		
	}

	void connectToZooKeeper(String zooKeeper) throws Exception {
		try {
			curator = CuratorFrameworkFactory.newClient(zooKeeper, 1000, 15000,
					new RetryNTimes(5, 2000));
			curator.start();
			logger.info("Connection to Kafka Zookeeper successfull");
		} catch (Exception e) {
			logger.fatal("Failed to connect to Zookeer. Throwing the exception. Error message is::" + e.getMessage());	
			throw e;
		}
	}

	public void initConsumer() throws Exception{
		try{
			this.simpleConsumer = new SimpleConsumer(this.leadBrokerHost, this.leadBrokerPort, 5000, 1024 * 1024 * 10, this.clientName);
			logger.info("Successfully initialized Kafka Consumer");
		}
		catch(Exception e){
			logger.fatal("Failed to initialize Kafka Consumer. Throwing the Error. Error Message is::" + e.getMessage());
			throw e;
		}
	}
	//This method is NOT USED. Revisit to delete
	public void save(String path, String data) throws Exception {
		try {
			if (curator.checkExists().forPath(path) == null) {
				curator.create().creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT)
						.forPath(path, data.getBytes());
			} else {
				curator.setData().forPath(path, data.getBytes());
			}
		} catch (Exception e) {
			//logger.error("Error trying to save" + e.getMessage());
			throw e;
		}
	}
	
	//This method is NOT USED. Revisit to delete
	public String get(String path) {
		try {
			if (curator.checkExists().forPath(path) != null) {
				return new String(curator.getData().forPath(path));
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	//This method is NOT USED. Revisit to delete
	public void saveOffset(String topic, int partition, long offset) throws Exception {
		save(String.format("/es-river-kafka/offsets/%s/%s/%d",
				this.leadBrokerURL, topic, partition), Long.toString(offset));
	}


	public short saveOffsetInKafka(long offset, short errorCode) throws Exception{
		logger.info("Starting to save the Offset value to Kafka. Offset value is::" + offset + " and errorCode passed is::" + errorCode);
		short versionID = 0;
		int correlationId = 0;
		try{
			TopicAndPartition tp = new TopicAndPartition(this.topic, this.partition);
			OffsetMetadataAndError offsetMetaAndErr = new OffsetMetadataAndError(offset, OffsetMetadataAndError.NoMetadata(), errorCode);
			Map<TopicAndPartition, OffsetMetadataAndError> mapForCommitOffset = new HashMap<TopicAndPartition, OffsetMetadataAndError>();
			mapForCommitOffset.put(tp, offsetMetaAndErr);
			kafka.javaapi.OffsetCommitRequest offsetCommitReq = new kafka.javaapi.OffsetCommitRequest(this.clientName, mapForCommitOffset, versionID, correlationId, this.clientName);
			OffsetCommitResponse offsetCommitResp = this.simpleConsumer.commitOffsets(offsetCommitReq);
			logger.info("Completed OffsetSet commit. OffsetCommitResponse ErrorCode is::" + offsetCommitResp.errors().get(tp)+ "Returning to the caller.");
			return (Short) offsetCommitResp.errors().get(tp);
		}
		catch(Exception e){
			logger.fatal("Error when committing Offset to Kafka. Throwing the error. Error Message is::" + e.getMessage());
			throw e;
		}
	}
	
	public long fetchCurrentOffsetFromKafka() throws Exception{
		logger.info("Starting to fetchCurrentOffsetFromKafka");
		short versionID = 0;
		int correlationId = 0;
		try{
			List<TopicAndPartition> tp = new ArrayList<TopicAndPartition>(); 
			tp.add(new TopicAndPartition(this.topic, this.partition));	
			OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(this.clientName,tp,versionID,correlationId,this.clientName);
			OffsetFetchResponse response = this.simpleConsumer.fetchOffsets(offsetFetchReq);
			logger.info("Completed fetchCurrentOffsetFromKafka. CurrentOffset Fetched is::" + response.offsets().get(tp.get(0)).offset());
			return response.offsets().get(tp.get(0)).offset();
		}
		catch(Exception e){
			logger.fatal("Error when fetching current offset from kafka. Throwing the exception. Error Message is::" + e.getMessage());
			throw e;
		}
	}
	
	private PartitionMetadata findLeader() throws Exception {
		logger.info("Starting to find the leader broker for Kafka");
		PartitionMetadata returnMetaData = null;
		SimpleConsumer leadFindConsumer = null;
		try {
			leadFindConsumer = new SimpleConsumer(this.incomingBrokerHost, this.incomingBrokerPort, FIND_KAFKA_LEADER_TIMEOUT,
					this.consumerConfig.bulkSize, "leaderLookup");
			List<String> topics = new ArrayList<String>();
			topics.add(this.topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse resp = leadFindConsumer.send(req);

			List<TopicMetadata> metaData = resp.topicsMetadata();
			for (TopicMetadata item : metaData) {
				for (PartitionMetadata part : item.partitionsMetadata()) {
					if (part.partitionId() == this.partition) {
						System.out.println("ITS TRUE");
						returnMetaData = part;
					break;
					}
				}
			}
		} catch (Exception e) {
			logger.fatal("Error communicating with Broker [" + this.incomingBrokerHost
					+ "] to find Leader for [" + this.topic + ", " + this.partition
					+ "] Reason: " + e.getMessage());
			throw e;
		} finally {
			if (leadFindConsumer != null)
				leadFindConsumer.close();
			logger.info("closed the connection");
			
		}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(replica.host());
			}
		}
		
		this.leadBrokerHost = returnMetaData.leader().host();
		this.leadBrokerPort = returnMetaData.leader().port();
		this.leadBrokerURL = this.leadBrokerHost + ":" + this.leadBrokerPort;
		logger.info("Computed leadBrokerURL is::" + this.leadBrokerURL + ", Returning.");
		return returnMetaData;
	}

	
	public PartitionMetadata findNewLeader() throws Exception {
		logger.info("Starting to findNewLeader for the kafka");
		for (int i = 0; i < 3; i++) {
			logger.info("Attempt # ::" + i);
			boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
            	logger.info("MetaData is Empty, going to sleep");
            	goToSleep = true;
            } else if (metadata.leader() == null) {
            	logger.info("MetaData leader is Empty, going to sleep");
            	goToSleep = true;
            } else if (this.leadBrokerHost.equalsIgnoreCase(metadata.leader().host()) && i <= 1) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
            	this.leadBrokerHost = metadata.leader().host();
        		this.leadBrokerPort = metadata.leader().port();
        		this.leadBrokerURL = this.leadBrokerHost + ":" + this.leadBrokerPort;
        		logger.info("Found and Computed leadBrokerURL is::" + leadBrokerURL + ", returning");
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
		logger.fatal("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
	
	
	public long getLatestOffset() throws Exception {
		Long latestOffset;
		logger.info("Trying to get the LatestOffset for the topic: " + this.clientName);
		try{
			latestOffset = this.getOffset(this.topic, this.partition, OffsetRequest.LatestTime(), this.clientName);
		}
		catch(Exception e){
			logger.fatal("Exception when trying to get the getLatestOffset. Throwing the exception. Error is:" + e.getMessage());
			throw e;
		}
		logger.info("LatestOffset is::" + latestOffset);
		return latestOffset;
		
	}
	
	public long getOldestOffset() throws Exception {
		Long oldestOffset;
		logger.info("Trying to get the OldestOffset for the topic: " + this.clientName);
		try{
			oldestOffset = this.getOffset(this.topic, this.partition, OffsetRequest.EarliestTime(), this.clientName);
		}
		catch(Exception e){
			logger.fatal("Exception when trying to get the getOldestOffset. Throwing the exception. Error is:" + e.getMessage());
			throw e;
		}
		logger.info("oldestOffset is::" + oldestOffset);
		return oldestOffset;
	}
	
	private long getOffset(String topic, int partition, long whichTime, String clientName) throws Exception {
		logger.info("Starting the generic getOffset");
		try{
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
					partition);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
					whichTime, 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
					requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
					clientName);
			OffsetResponse response = this.simpleConsumer.getOffsetsBefore(request);

			if (response.hasError()) {
				logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
				throw new Exception("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			}
			long[] offsets = response.offsets(topic, partition);
			logger.info("Offset is::" + offsets[0]);
			return offsets[0];

		}
	catch(Exception e){
		logger.fatal("Exception when trying to get the Offset. Throwing the exception. Error is:" + e.getMessage());
		throw e;
	}
	}

	FetchResponse getFetchResponse(long offset, int maxSizeBytes) throws Exception {
		logger.info("Starting getFetchResponse");
		try{
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(this.topic, this.partition, offset, maxSizeBytes).build();
			FetchResponse fetchResponse = this.simpleConsumer.fetch(req);
			logger.info("Fetch from Kafka excecuted without Exception. Returning with the fetchResponse");
			return fetchResponse;
		}
		catch(Exception e){
			logger.fatal("Exception when trying to fetch the messages from Kafka. Throwing the exception. Error message is::" + e.getMessage());
			throw e;
		}


	}
	
	ByteBufferMessageSet fetchMessageSet(FetchResponse fetchReponse){
		return fetchReponse.messageSet(this.topic, this.partition);
	}
	
	public void close() {
		curator.close();
		logger.info("Curator/Zookeeper connection closed");
	}
	
}
