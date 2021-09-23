package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.values.KV;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

public class PulsarRecord<K, V> {

    private final String topic;
    private Message message;
    private Long offset;
    private KV<K,V> kv;

    public PulsarRecord(String topic, Message message, Long offset) {
        this.topic = topic;
        this.offset = offset;
        this.message = message;
    }

    public Message getMessage() { return this.message; }

    public Long getOffset() {
        return this.offset;
    }

    public String getTopic() {
        return this.topic;
    }



}
