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

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Checkpoint for an unbounded KafkaIO.Read. Consists of Kafka topic name, partition id,
 * and the latest offset consumed so far.
 */
@DefaultCoder(SerializableCoder.class)
public class KafkaCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private final List<PartitionMark> partitions;

  public KafkaCheckpointMark(List<PartitionMark> partitions) {
    this.partitions = partitions;
  }

  public List<PartitionMark> getPartitions() {
    return partitions;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    /* nothing to do */

    // We might want to support committing offset in Kafka for better resume point when the job
    // is restarted (checkpoint is not available for job restarts).
  }

  /**
   * A tuple to hold topic, partition, and offset that comprise the checkpoint
   * for a single partition.
   */
  public static class PartitionMark implements Serializable {
    private final TopicPartition topicPartition;
    private final long offset;

    public PartitionMark(TopicPartition topicPartition, long offset) {
      this.topicPartition = topicPartition;
      this.offset = offset;
    }

    public TopicPartition getTopicPartition() {
      return topicPartition;
    }

    public long getOffset() {
      return offset;
    }
  }
}

