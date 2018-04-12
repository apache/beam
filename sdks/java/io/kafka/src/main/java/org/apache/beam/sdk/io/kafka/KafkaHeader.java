package org.apache.beam.sdk.io.kafka;

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.header.Header}. Included here in order
 * to support older Kafka versions (0.9.x).
 */
public interface KafkaHeader {
  String key();

  byte[] value();
}
