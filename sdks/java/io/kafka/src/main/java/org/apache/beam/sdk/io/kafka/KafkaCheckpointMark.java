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
import org.apache.avro.reflect.AvroIgnore;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * Checkpoint for a {@link KafkaUnboundedReader}. Consists of Kafka topic name, partition id,
 * and the latest offset consumed so far.
 */
@DefaultCoder(AvroCoder.class)
public class KafkaCheckpointMark implements UnboundedSource.CheckpointMark {

  private List<PartitionMark> partitions;

  @AvroIgnore
  private KafkaUnboundedReader<?, ?> reader; // Non-null when offsets need to be committed.

  private KafkaCheckpointMark() {} // for Avro

  public KafkaCheckpointMark(List<PartitionMark> partitions,
                             KafkaUnboundedReader<?, ?> reader) {
    this.partitions = partitions;
    this.reader = reader;
  }

  public List<PartitionMark> getPartitions() {
    return partitions;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    if (reader != null) {
      // Is it ok to commit asynchronously, or should we wait till this (or newer) is committed?
      // Often multiple marks would be finalized at once, since we only need to finalize the latest,
      // it is better to wait a little while. Currently maximum is delay same as KAFKA_POLL_TIMEOUT
      // in the reader (1 second).
      reader.finalizeCheckpointMarkAsync(this);
    }
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

