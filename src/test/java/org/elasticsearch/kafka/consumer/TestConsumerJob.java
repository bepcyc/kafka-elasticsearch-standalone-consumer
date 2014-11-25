package org.elasticsearch.kafka.consumer;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by bepcyc on 11/25/14.
 */
public class TestConsumerJob extends TestCase {
    final Properties properties = new Properties();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        String propString = "zookeeper=10.121.16.21\n" +
                "brokerHost=10.121.16.21\n" +
                "brokerPort=9092\n" +
                "consumerGroupName=ESKafkaConsumerClient\n" +
                "topic=es_kafka\n" +
                "partition=0\n" +
                "startOffsetFrom=CUSTOM\n" +
                "startOffset=0\n" +
                "messageHandlerClass=org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler\n" +
                "esHost=10.121.16.21\n" +
                "esPort=9200\n" +
                "esIndex=kafka_shit\n" +
                "esIndexType=my_type\n" +
                "esMsgFailureTolerancePercent=5";
        final InputStream inputStream = new ByteArrayInputStream(propString.getBytes(StandardCharsets.UTF_8));
        properties.load(inputStream);
    }

    public void testConsumerConfig() {
        try {
            final ConsumerConfig config = new ConsumerConfig(this.properties);
            assertTrue("Config initialized", config != null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testConsumerJob() {
        try {
            final ConsumerConfig config = new ConsumerConfig(this.properties);
            final ConsumerJob job = new ConsumerJob(config);
            final long computeOffset = job.computeOffset();
            assertTrue("computed offset is positive", computeOffset >= 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
