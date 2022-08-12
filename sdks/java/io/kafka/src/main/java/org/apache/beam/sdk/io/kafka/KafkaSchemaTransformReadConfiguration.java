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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.kafka.common.TopicPartition;

/**
 * Configuration for reading from a Kafka topic.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Experimental
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class KafkaSchemaTransformReadConfiguration {

  /** Instantiates a {@link KafkaSchemaTransformReadConfiguration.Builder} instance. */
  public static Builder builder() {
    return new AutoValue_KafkaSchemaTransformReadConfiguration.Builder();
  }

  /** Sets the bootstrap servers for the Kafka consumer. */
  @Nullable
  public abstract String getBootstrapServers();

  /** Flags whether finalized offsets are committed to Kafka. */
  @Nullable
  public abstract Boolean getCommitOffsetsInFinalize();

  /** Configuration updates for the backend main consumer. */
  @Nullable
  public abstract Map<String, Object> getConsumerConfigUpdates();

  /**
   * Sets the timestamps policy based on KafkaTimestampType.CREATE_TIME timestamp of the records.
   */
  @Nullable
  public abstract Long getCreateTimeMillisecondsMaximumDelay();

  /**
   * Configure the KafkaIO to use WatchKafkaTopicPartitionDoFn to detect and emit any new available
   * {@link TopicPartition} for ReadFromKafkaDoFn to consume during pipeline execution time.
   */
  @Nullable
  public abstract Long getDynamicReadMillisecondsDuration();

  /** Additional configuration for the backend offset consumer. */
  @Nullable
  public abstract Map<String, Object> getOffsetConsumerConfiguration();

  /** Specifies whether to include metadata when reading from Kafka topic. */
  @Nullable
  public abstract Boolean getReadWithMetadata();

  /** Sets "isolation_level" to "read_committed" in Kafka consumer configuration. */
  @Nullable
  public abstract Boolean getReadCommitted();

  /** Use timestamp to set up start offset. */
  @Nullable
  public abstract Long getStartReadTimeMillisecondsEpoch();

  /** Use timestamp to set up stop offset. */
  @Nullable
  public abstract Long getStopReadTimeMillisecondsEpoch();

  /**
   * A timestamp policy to assign event time for messages in a Kafka partition and watermark for it.
   */
  @Nullable
  public abstract TimestampPolicyConfiguration getTimestampPolicy();

  /** Sets the topic from which to read. */
  @Nullable
  public abstract String getTopic();

  /** Kafka partitions from which to read. */
  @Nullable
  public abstract List<TopicPartitionConfiguration> getTopicPartitions();

  /** Builder for the {@link KafkaSchemaTransformReadConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the bootstrap servers for the Kafka consumer. */
    public abstract Builder setBootstrapServers(String value);

    /** Flags whether finalized offsets are committed to Kafka. */
    public abstract Builder setCommitOffsetsInFinalize(Boolean value);

    /** Configuration updates for the backend main consumer. */
    public abstract Builder setConsumerConfigUpdates(Map<String, Object> value);

    /**
     * Sets the timestamps policy based on KafkaTimestampType.CREATE_TIME timestamp of the records.
     */
    public abstract Builder setCreateTimeMillisecondsMaximumDelay(Long value);

    /**
     * Configure the KafkaIO to use WatchKafkaTopicPartitionDoFn to detect and emit any new
     * available {@link TopicPartition} for ReadFromKafkaDoFn to consume during pipeline execution
     * time.
     */
    public abstract Builder setDynamicReadMillisecondsDuration(Long value);

    /** Additional configuration for the backend offset consumer. */
    public abstract Builder setOffsetConsumerConfiguration(Map<String, Object> value);

    /** Specifies whether to include metadata when reading from Kafka topic. */
    public abstract Builder setReadWithMetadata(Boolean value);

    /** Sets "isolation_level" to "read_committed" in Kafka consumer configuration. */
    public abstract Builder setReadCommitted(Boolean value);

    /** Use timestamp to set up start offset. */
    public abstract Builder setStartReadTimeMillisecondsEpoch(Long value);

    /** Use timestamp to set up stop offset. */
    public abstract Builder setStopReadTimeMillisecondsEpoch(Long value);

    /**
     * A timestamp policy to assign event time for messages in a Kafka partition and watermark for
     * it.
     */
    public abstract Builder setTimestampPolicy(TimestampPolicyConfiguration value);

    /** Sets the topic from which to read. */
    public abstract Builder setTopic(String value);

    /** Kafka partitions from which to read. */
    public abstract Builder setTopicPartitions(List<TopicPartitionConfiguration> value);

    /** Builds a {@link KafkaSchemaTransformReadConfiguration} instance. */
    public abstract KafkaSchemaTransformReadConfiguration build();
  }

  /**
   * A configuration for a {@link TopicPartition}.
   *
   * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
   * provide no backwards compatibility guarantees, and it should not be implemented outside the
   * Beam repository.
   */
  @Experimental
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class TopicPartitionConfiguration {

    /** Instantiates a {@link TopicPartitionConfiguration.Builder} instance. */
    public static Builder builder() {
      return new AutoValue_KafkaSchemaTransformReadConfiguration_TopicPartitionConfiguration
          .Builder();
    }

    /** The name of the topic defining the partition. */
    public abstract String getTopic();

    /** The number of the topic partition. */
    public abstract Integer getPartition();

    /** Builder for the {@link TopicPartitionConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

      /** The name of the topic defining the partition. */
      public abstract Builder setTopic(String value);

      /** The number of the topic partition. */
      public abstract Builder setPartition(Integer value);

      /** Builds a {@link TopicPartitionConfiguration} instance. */
      public abstract TopicPartitionConfiguration build();
    }
  }

  /**
   * A timestamp policy to assign event time for messages in a Kafka partition and watermark for it.
   *
   * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
   * provide no backwards compatibility guarantees, and it should not be implemented outside the
   * Beam repository.
   */
  @Experimental
  public enum TimestampPolicyConfiguration {

    /**
     * Assigns Kafka's log append time (server side ingestion time) to each record. The watermark
     * for each Kafka partition is the timestamp of the last record read. If a partition is idle,
     * the watermark advances roughly to 'current time - 2 seconds'. See {@link
     * KafkaIO.Read#withLogAppendTime()} for longer description.
     */
    LOG_APPEND_TIME,

    /**
     * A simple policy that uses current time for event time and watermark. This should be used when
     * better timestamps like LogAppendTime are not available for a topic.
     */
    PROCESSING_TIME,
  }
}
