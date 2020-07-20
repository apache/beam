/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import java.util.Arrays;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.kafka.common.header.Headers;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * KafkaRecord contains key and value of the record as well as metadata for the record (topic name,
 * partition id, and offset).
 */
public class KafkaRecord<K, V> {
  // This is based on {@link ConsumerRecord} received from Kafka Consumer.
  // The primary difference is that this contains deserialized key and value, and runtime
  // Kafka version agnostic (e.g. Kafka version 0.9.x does not have timestamp fields).

  private final String topic;
  private final int partition;
  private final long offset;
  private final Headers headers;
  private final KV<K, V> kv;
  private final long timestamp;
  private final KafkaTimestampType timestampType;

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      long timestamp,
      KafkaTimestampType timestampType,
      @Nullable Headers headers,
      K key,
      V value) {
    this(topic, partition, offset, timestamp, timestampType, headers, KV.of(key, value));
  }

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      long timestamp,
      KafkaTimestampType timestampType,
      @Nullable Headers headers,
      KV<K, V> kv) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.timestampType = timestampType;
    this.headers = headers;
    this.kv = kv;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public Headers getHeaders() {
    if (!ConsumerSpEL.hasHeaders()) {
      throw new RuntimeException(
          "The version kafka-clients does not support record headers, "
              + "please use version 0.11.0.0 or newer");
    }
    return headers;
  }

  public KV<K, V> getKV() {
    return kv;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public KafkaTimestampType getTimestampType() {
    return timestampType;
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[] {topic, partition, offset, timestamp, headers, kv});
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KafkaRecord) {
      @SuppressWarnings("unchecked")
      KafkaRecord<Object, Object> other = (KafkaRecord<Object, Object>) obj;
      return topic.equals(other.topic)
          && partition == other.partition
          && offset == other.offset
          && timestamp == other.timestamp
          && Objects.equal(headers, other.headers)
          && kv.equals(other.kv);
    } else {
      return false;
    }
  }
}
