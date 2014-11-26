package org.elasticsearch.kafka.consumer;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by Viacheslav Rodionov (viacheslav.rodionov@gmail.com) on 11/25/14.
 */
public class TestConsumerJob extends TestCase {
    final Properties properties = new Properties();

    /**
     * TODO: make it work with embedded kafka. Now it's a local one.
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final String propString = "zookeeper=localhost\n" +
                "brokerHost=localhost\n" +
                "brokerPort=9092\n" +
                "consumerGroupName=ESKafkaConsumerClient\n" +
                "topic=es_kafka\n" +
                "partition=0\n" +
                "startOffsetFrom=OLDEST\n" +
                //"startOffset=0\n" +
                "messageHandlerClass=org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler\n" +
                "esHost=localhost\n" +
                "esPort=9200\n" +
                "esIndex=kafka_idx\n" +
                "esIndexType=my_type\n" +
                "esMsgFailureTolerancePercent=5";
        final InputStream inputStream = new ByteArrayInputStream(propString.getBytes(StandardCharsets.UTF_8));
        properties.load(inputStream);
    }

    @Test
    public void testConsumerConfig() throws Exception {
        try {
            final ConsumerConfig config = new ConsumerConfig(properties);
            assertTrue("Config initialized", config != null);
        } catch (final Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testConsumerJob() throws Exception {
        try {
            final ConsumerConfig config = new ConsumerConfig(properties);
            final ConsumerJob job = new ConsumerJob(config);
            final long computeOffset = job.computeOffset();
            assertTrue("computed offset is positive", computeOffset >= 0);
        } catch (final Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
