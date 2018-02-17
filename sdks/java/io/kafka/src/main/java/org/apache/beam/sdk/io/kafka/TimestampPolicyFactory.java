/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF E 2.0 (the
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
import java.util.Optional;
import org.apache.beam.sdk.io.kafka.TimestampPolicy.PartitionContext;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An extendable factory to create a {@link TimestampPolicy} for each partition at runtime by
 * KafkaIO reader. Subclasses implement {@link #createTimestampPolicy}, which is invoked by
 * the the reader while starting or resuming from a checkpoint. Two commonly used policies are
 * provided. See {@link #withLogAppendTime()} and {@link #withProcessingTime()}.
 */
public abstract class TimestampPolicyFactory<KeyT, ValueT> implements Serializable {

  /**
   * Creates a TimestampPolicy for a partition. This is invoked by the reader at the start or while
   * resuming from previous checkpoint.
   *
   * @param tp The returned policy applies to records from this {@link TopicPartition}.
   * @param previousWatermark The latest check-pointed watermark. This is set when the reader
   *           is resuming from a checkpoint. This is a good value to return by implementations
   *           of {@link TimestampPolicy#getWatermark(PartitionContext)} until a better watermark
   *           can be established as more records are read.
   * @return
   */
  public abstract TimestampPolicy<KeyT, ValueT> createTimestampPolicy(
    TopicPartition tp, Optional<Instant> previousWatermark);

  /**
   * A {@link TimestampPolicy} that assigns processing time to each record.
   * Specifically, this is the timestamp when the record becomes 'current' in the reader.
   * The watermark aways advances to current time.
   */
  public static <K, V> TimestampPolicyFactory<K, V> withProcessingTime() {
    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V>
      createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new ProcessingTimePolicy<>();
      }
    };
  }

  /**
   * A {@link TimestampPolicy} that assigns Kafka's log append time (server side ingestion time)
   * to each record. The watermark for each Kafka partition is the timestamp of the last record
   * read. If a partition is idle, the watermark advances roughly to 'current time - 2 seconds'.
   * See {@link KafkaIO.Read#withLogAppendTime()} for longer description.
   */
  public static <K, V> TimestampPolicyFactory<K, V> withLogAppendTime() {
    //return (tp, previousWatermark) -> new LogAppendTimePolicy<>(previousWatermark);
    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V>
      createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new LogAppendTimePolicy<>(previousWatermark);
      }
    };
  }

  /*
   * TODO
   * Provide a another built in implementation where the watermark is based on all the timestamps
   * seen in last 1 minute of wall clock time (this duration could be configurable). This is
   * similar to watermark set by PubsubIO.
   *
   * public static <K, V> TimestampPolicyFactory<K, V> withCreateTime() {
   *   return withCustomTypestamp(...);
   * }
   *
   * public static <K, V> TimestampPolicyFactory<K, V> withCustomTimestamp() {
   * }
   */

  /**
   * Used by the Read transform to support old timestamp functions API.
   */
  static <K, V> TimestampPolicyFactory<K, V> withTimestampFn(
    final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {

    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V> createTimestampPolicy(TopicPartition tp,
                                                         Optional<Instant> previousWatermark) {
        return new TimestampFnPolicy<>(timestampFn, previousWatermark);
      }
    };
  }

  /**
   * A simple policy that uses current time for event time and watermark. This should be used
   * when better timestamps like LogAppendTime are not available for a topic.
   */
  public static class ProcessingTimePolicy<K, V> extends TimestampPolicy<K, V> {

    @Override
    public Instant getTimestampForRecord(PartitionContext context, KafkaRecord<K, V> record) {
      return Instant.now();
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      return Instant.now();
    }
  }

  /**
   * Assigns Kafka's log append time (server side ingestion time)
   * to each record. The watermark for each Kafka partition is the timestamp of the last record
   * read. If a partition is idle, the watermark advances roughly to 'current time - 2 seconds'.
   * See {@link KafkaIO.Read#withLogAppendTime()} for longer description.
   */
  public static class LogAppendTimePolicy<K, V> extends TimestampPolicy<K, V> {

    /**
     * When a partition is idle or caught up (i.e. backlog is zero), we advance the watermark
     * to near realtime. Kafka server does not have an API to provide server side current
     * timestamp which could ensure minimum LogAppendTime for future records.
     * The best we could do is to advance the watermark to
     * 'last backlog check time - small delta to account for any internal buffering in Kafka'.
     * Using 2 seconds for this delta. Should this be user configurable?
     */
    private static final Duration IDLE_WATERMARK_DELTA = Duration.standardSeconds(2);

    protected Instant currentWatermark;

    public LogAppendTimePolicy(Optional<Instant> previousWatermark) {
      currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant getTimestampForRecord(PartitionContext context, KafkaRecord<K, V> record) {
      if (record.getTimestampType().equals(KafkaTimestampType.LOG_APPEND_TIME)) {
        currentWatermark = new Instant(record.getTimestamp());
      } else if (currentWatermark.equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
        // This is the first record and it does not have LOG_APPEND_TIME.
        // Most likely the topic is not configured correctly.
        throw new IllegalStateException(String.format(
          "LogAppendTimePolicy policy is enabled in reader, but Kafka record's timestamp type "
          + "is LogAppendTime. Most likely it is not enabled on Kafka for the topic '%s'. "
          + "Actual timestamp type is '%s'.", record.getTopic(), record.getTimestampType()));
      }
      return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      if (context.getMessageBacklog() == 0) {
        // The reader is caught up. May need to advance the watermark.
        Instant idleWatermark = context.getBacklogCheckTime().minus(IDLE_WATERMARK_DELTA);
        if (idleWatermark.isAfter(currentWatermark)) {
          currentWatermark = idleWatermark;
        }
      } // else, there is backlog (or is unknown). Do not advance the watermark.
      return currentWatermark;
    }
  }

  /**
   * Internal policy to support deprecated withTimestampFn API. It returns last record
   * timestamp for watermark!.
   */
  private static class TimestampFnPolicy<K, V> extends TimestampPolicy<K, V> {

    final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    Instant lastRecordTimestamp;

    TimestampFnPolicy(SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
                      Optional<Instant> previousWatermark) {
      this.timestampFn = timestampFn;
      lastRecordTimestamp = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant getTimestampForRecord(PartitionContext context, KafkaRecord<K, V> record) {
      lastRecordTimestamp = timestampFn.apply(record);
      return lastRecordTimestamp;
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      return lastRecordTimestamp;
    }
  }
}
