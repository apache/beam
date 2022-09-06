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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

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

  public static final Set<String> VALID_START_OFFSET_VALUES = Sets.newHashSet("earliest", "latest");

  public void validate() {
    assert this.getStartOffset() == null
            || VALID_START_OFFSET_VALUES.contains(this.getStartOffset())
        : "Valid Kafka Start offset values are " + VALID_START_OFFSET_VALUES.toString();
  }

  /** Instantiates a {@link KafkaSchemaTransformReadConfiguration.Builder} instance. */
  public static Builder builder() {
    return new AutoValue_KafkaSchemaTransformReadConfiguration.Builder();
  }

  /** Sets the bootstrap servers for the Kafka consumer. */
  public abstract String getBootstrapServers();

  /**
   * A timestamp policy to assign event time for messages in a Kafka partition and watermark for it.
   */
  @Nullable
  public abstract TimestampPolicyConfiguration getTimestampPolicy();

  @Nullable
  public abstract String getConfluentSchemaRegistryUrl();

  // TODO(pabloem): Make data format an ENUM
  @Nullable
  public abstract String getDataFormat();

  @Nullable
  public abstract String getConfluentSchemaRegistrySubject();

  @Nullable
  public abstract String getAvroSchema();

  @Nullable
  public abstract String getStartOffset();

  /** Sets the topic from which to read. */
  public abstract String getTopic();

  /** Builder for the {@link KafkaSchemaTransformReadConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the bootstrap servers for the Kafka consumer. */
    public abstract Builder setBootstrapServers(String value);

    public abstract Builder setTimestampPolicy(TimestampPolicyConfiguration timestampPolicy);

    public abstract Builder setConfluentSchemaRegistryUrl(String schemaRegistry);

    public abstract Builder setConfluentSchemaRegistrySubject(String subject);

    public abstract Builder setAvroSchema(String schema);

    public abstract Builder setDataFormat(String dataFormat);

    public abstract Builder setStartOffset(String startOffset);

    /** Sets the topic from which to read. */
    public abstract Builder setTopic(String value);

    /** Builds a {@link KafkaSchemaTransformReadConfiguration} instance. */
    public abstract KafkaSchemaTransformReadConfiguration build();
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
