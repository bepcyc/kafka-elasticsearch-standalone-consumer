package org.elasticsearch.kafka.consumer.mappers;

public class KafkaMetaDataMapper {

    private String topic;
    private String consumerGroupName;
    private short partition;
    private long offset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(final String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public short getPartition() {
        return partition;
    }

    public void setPartition(final short partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

}
