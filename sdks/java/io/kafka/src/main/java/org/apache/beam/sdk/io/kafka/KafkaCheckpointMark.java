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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * Checkpoint for an unbounded KafkaIO.Read. Consists of Kafka topic name, partition id,
 * and the latest offset consumed so far.
 */
@DefaultCoder(AvroCoder.class)
public class KafkaCheckpointMark implements UnboundedSource.CheckpointMark {

  private List<PartitionMark> partitions;

  private KafkaCheckpointMark() {} // for Avro

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

  @Override
  public String toString() {
    return "KafkaCheckpointMark{partitions=" + Joiner.on(",").join(partitions) + '}';
  }

  /**
   * A tuple to hold topic, partition, and offset that comprise the checkpoint
   * for a single partition.
   */
  public static class PartitionMark implements Serializable {
    private String topic;
    private int partition;
    private long nextOffset;

    private PartitionMark() {} // for Avro

    public PartitionMark(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.nextOffset = offset;
    }

    public String getTopic() {
      return topic;
    }

    public int getPartition() {
      return partition;
    }

    public long getNextOffset() {
      return nextOffset;
    }

    @Override
    public String toString() {
      return "PartitionMark{"
          + "topic='" + topic + '\''
          + ", partition=" + partition
          + ", nextOffset=" + nextOffset
          + '}';
    }
  }
}

