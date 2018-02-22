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

import org.apache.beam.sdk.io.UnboundedSource;
import org.joda.time.Instant;

/**
 * A timestamp policy to assign event time for messages in a Kafka partition and watermark for it.
 * KafkaIO reader creates one policy using {@link TimestampPolicyFactory} for each each of the
 * partitions it reads from. See @{@link TimestampPolicyFactory.LogAppendTimePolicy} for example
 * of a policy.
 */
public abstract class TimestampPolicy<K, V> {

  /**
   * The context contains state maintained in the reader for the partition. Available with
   * each of the methods in @{@link TimestampPolicy}.
   */
  public abstract static class PartitionContext {
    /**
     * Current backlog in messages
     * (latest offset of the partition - last processed record offset).
     */
    public abstract long getMessageBacklog();

    /**
     * The time at which latest offset for the partition was fetched in order to calculate
     * backlog. The reader periodically polls for latest offsets. This timestamp
     * is useful in advancing watermark for idle partitions as in
     * {@link TimestampPolicyFactory.LogAppendTimePolicy}.
     */
    public abstract Instant getBacklogCheckTime();
  }

  /**
   * Returns record timestamp (aka event time). This is often based on the timestamp
   * of the Kafka record. This is invoked for each record when it is processed in the reader.
   */
  public abstract Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<K, V> record);

  /**
   * Returns watermark for the partition. It is the timestamp before or at the timestamps of all
   * future records consumed from the partition.
   * See {@link UnboundedSource.UnboundedReader#getWatermark()} for more guidance on watermarks.
   * E.g. if the record timestamp is 'LogAppendTime', watermark would be the timestamp of the last
   * record since 'LogAppendTime' monotonically increases within a partition.
   */
  public abstract Instant getWatermark(PartitionContext ctx);

  // It is useful to let TimestampPolicy store its state in checkpointMark.
  // We need to add a getCheckpointMark() here for that.
}
