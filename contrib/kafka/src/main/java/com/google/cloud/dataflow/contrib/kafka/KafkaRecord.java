/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.kafka;

import com.google.cloud.dataflow.sdk.values.KV;

import java.io.Serializable;

/**
 * KafkaRecord contains key and value of the record as well as metadata for the record (topic name,
 * partition id, and offset).
 */
public class KafkaRecord<K, V> implements Serializable {

  private final String topic;
  private final int partition;
  private final long offset;
  private final KV<K, V> kv;

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      K key,
      V value) {
    this(topic, partition, offset, KV.of(key, value));
  }

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      KV<K, V> kv) {

    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
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

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KafkaRecord) {
      @SuppressWarnings("unchecked")
      KafkaRecord<Object, Object> other = (KafkaRecord<Object, Object>) obj;
      return topic.equals(other.topic)
          && partition == other.partition
          && offset == other.offset
          && kv.equals(other.kv);
    } else {
      return false;
    }
  }
}
