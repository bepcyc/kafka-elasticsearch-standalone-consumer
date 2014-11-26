package org.elasticsearch.kafka.consumer;

import org.elasticsearch.client.Client;

import java.util.LinkedHashMap;


public interface MessageHandlerFactory {

    public MessageHandler createMessageHandler(Client client, ConsumerConfig config, LinkedHashMap<Long, ? extends Object> offsetMsgMap);

    public void transformMessage() throws Exception;

    public void prepareForPostToElasticSearch() throws Exception;

    public boolean postToElasticSearch() throws Exception;


}
