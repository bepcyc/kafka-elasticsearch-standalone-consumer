package org.elasticsearch.kafka.consumer.messageHandlers;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.kafka.consumer.ConsumerLogger;
import org.elasticsearch.kafka.consumer.MessageHandler;

import java.nio.ByteBuffer;

public class RawMessageStringHandler extends MessageHandler {

    private final Logger logger = ConsumerLogger.getLogger(this.getClass());

    public RawMessageStringHandler() {
        super();
        logger.info("Initialized RawMessageStringHandler");
    }

    @Override
    public void transformMessage() throws Exception {
        logger.info("Starting to transformMessages into String Messages");
        for (final Long offsetKey : getOffsetMsgMap().keySet()) {
            final ByteBuffer payload = getOffsetMsgMap().get(offsetKey).payload();
            final byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            getEsPostObject().add(new String(bytes, "UTF-8"));
        }
        logger.info("Completed transforming Messages into String Messages");
    }

    @Override
    public void prepareForPostToElasticSearch() {
        logger.info("Starting prepareForPostToElasticSearch");
        final BulkRequestBuilder buildReqBuilder = getEsClient().prepareBulk();
        for (final Object eachMsg : getEsPostObject()) {
            buildReqBuilder.add(getEsClient().prepareIndex(getConfig().getEsIndex(), getConfig().getEsIndexType()).setSource((String) eachMsg));
        }
        setBuildReqBuilder(buildReqBuilder);
    }

}
