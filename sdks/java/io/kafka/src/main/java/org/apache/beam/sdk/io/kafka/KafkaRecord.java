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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.beam.sdk.values.KV;

/**
 * KafkaRecord contains key and value of the record as well as metadata for the record (topic name,
 * partition id, and offset).
 */
public class KafkaRecord<K, V> implements Serializable {

  private final String topic;
  private final int partition;
  private final long offset;
  private final KV<K, V> kv;
  private final long timestamp;

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      long timestamp,
      K key,
      V value) {
    this(topic, partition, offset, timestamp, KV.of(key, value));
  }

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      long timestamp,
      KV<K, V> kv) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
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

  public KV<K, V> getKV() {
    return kv;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[]{topic, partition, offset, timestamp, kv});
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
          && kv.equals(other.kv);
    } else {
      return false;
    }
  }
}
