package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.values.KV;
import org.apache.pulsar.client.api.MessageId;

public class PulsarRecord<K, V> {

    private final String topic;
    private MessageId messageId;
    private Long offset;
    private KV<K,V> kv;

    public PulsarRecord(String topic, MessageId messageId, Long offset) {
        this.topic = topic;
        this.offset = offset;
    }

    public MessageId getMessageId() { return this.messageId; }

    public Long getOffset() {
        return this.offset;
    }

    public String getTopic() {
        return this.topic;
    }



}
