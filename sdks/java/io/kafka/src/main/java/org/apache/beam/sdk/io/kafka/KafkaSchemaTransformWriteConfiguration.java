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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for writing to a Kafka topic.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Experimental
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class KafkaSchemaTransformWriteConfiguration {

  /** The Kafka topic to which to write. */
  public abstract String getTopic();

  /**
   * Flags whether the timestamp for each record being published is set to timestamp of the element
   * in the pipeline.
   */
  @Nullable
  public abstract Boolean getApplyInputTimestamp();

  /** The bootstrap servers for the Kafka producer. */
  @Nullable
  public abstract String getBootstrapServers();

  /**
   * The name of the {@link org.apache.kafka.common.serialization.Serializer} for the key (if any)
   * to bytes.
   */
  @Nullable
  public abstract String getKeySerializer();

  /** The configuration of the producer. */
  @Nullable
  public abstract Map<String, Object> getProducerConfiguration();

  /**
   * The name of the {@link org.apache.kafka.common.serialization.Serializer} for the serializing
   * the value to bytes.
   */
  @Nullable
  public abstract String getValueSerializer();

  @AutoValue.Builder
  public abstract static class Builder {

    /** The Kafka topic to which to write. */
    public abstract Builder setTopic(String value);

    /**
     * Flags whether the timestamp for each record being published is set to timestamp of the
     * element in the pipeline.
     */
    public abstract Builder setApplyInputTimestamp(Boolean value);

    /** The bootstrap servers for the Kafka producer. */
    public abstract Builder setBootstrapServers(String value);

    /**
     * The name of the {@link org.apache.kafka.common.serialization.Serializer} for the key (if any)
     * to bytes.
     */
    public abstract Builder setKeySerializer(String value);

    /** The configuration of the producer. */
    public abstract Builder setProducerConfiguration(Map<String, Object> value);

    /**
     * The name of the {@link org.apache.kafka.common.serialization.Serializer} for the serializing
     * the value to bytes.
     */
    public abstract Builder setValueSerializer(String value);

    /** Builds the {@link KafkaSchemaTransformWriteConfiguration}. */
    public abstract KafkaSchemaTransformWriteConfiguration build();
  }
}
