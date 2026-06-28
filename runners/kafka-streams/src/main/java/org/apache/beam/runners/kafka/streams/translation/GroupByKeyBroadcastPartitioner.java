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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Partitions records on the GroupByKey repartition topic.
 *
 * <ul>
 *   <li><b>data</b> records go to the single partition selected by hashing the (already encoded
 *       Beam key) Kafka record key — the same scheme Kafka's default partitioner uses — so every
 *       value of a key lands together;
 *   <li><b>watermark</b> reports are broadcast to <i>every</i> partition, so each downstream
 *       GroupByKey task observes the terminal watermark and fires its keys.
 * </ul>
 *
 * @param <T> the data element type carried by data payloads
 */
class GroupByKeyBroadcastPartitioner<T> implements StreamPartitioner<byte[], KStreamsPayload<T>> {

  @Override
  public Integer partition(String topic, byte[] key, KStreamsPayload<T> value, int numPartitions) {
    // Required by the interface but unused: Kafka Streams calls partitions() (overridden below)
    // when it is present. Kept consistent with the data-hash path for safety.
    return key == null ? 0 : Utils.toPositive(Utils.murmur2(key)) % numPartitions;
  }

  @Override
  public Optional<Set<Integer>> partitions(
      String topic, byte[] key, KStreamsPayload<T> value, int numPartitions) {
    if (value.isWatermark()) {
      Set<Integer> all = new HashSet<>();
      for (int partition = 0; partition < numPartitions; partition++) {
        all.add(partition);
      }
      return Optional.of(all);
    }
    int partition = Utils.toPositive(Utils.murmur2(key)) % numPartitions;
    return Optional.of(Collections.singleton(partition));
  }
}
