package org.apache.beam.sdk.io.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@link Serializer} for {@link Void}.
 *
 * <p>Will always serialize the given data as null.
 * This is useful for {@link Producer} where the key is always null.
 */
public class VoidSerializer<V> implements Serializer<V> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, V data) {
        return null;
    }

    @Override
    public void close() {
    }
}
