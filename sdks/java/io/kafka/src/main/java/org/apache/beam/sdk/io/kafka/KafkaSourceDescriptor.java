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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Instant;

/** Represents a Kafka source description. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class KafkaSourceDescriptor implements Serializable {
  @SchemaFieldName("topic")
  @Pure
  abstract String getTopic();

  @SchemaFieldName("partition")
  @Pure
  abstract Integer getPartition();

  @SchemaFieldName("start_read_offset")
  @Pure
  abstract @Nullable Long getStartReadOffset();

  @SchemaFieldName("start_read_time")
  @Pure
  abstract @Nullable Instant getStartReadTime();

  @SchemaFieldName("stop_read_offset")
  @Pure
  abstract @Nullable Long getStopReadOffset();

  @SchemaFieldName("stop_read_time")
  @Pure
  abstract @Nullable Instant getStopReadTime();

  @SchemaFieldName("bootstrap_servers")
  @Pure
  abstract @Nullable List<String> getBootStrapServers();

  private @Nullable TopicPartition topicPartition = null;

  @SchemaIgnore
  @Deterministic
  public TopicPartition getTopicPartition() {
    if (topicPartition == null) {
      topicPartition = new TopicPartition(getTopic(), getPartition());
    }
    return topicPartition;
  }

  public static KafkaSourceDescriptor of(
      TopicPartition topicPartition,
      @Nullable Long startReadOffset,
      @Nullable Instant startReadTime,
      @Nullable Long stopReadOffset,
      @Nullable Instant stopReadTime,
      @Nullable List<String> bootstrapServers) {
    checkArguments(startReadOffset, startReadTime, stopReadOffset, stopReadTime);
    return new AutoValue_KafkaSourceDescriptor(
        topicPartition.topic(),
        topicPartition.partition(),
        startReadOffset,
        startReadTime,
        stopReadOffset,
        stopReadTime,
        bootstrapServers);
  }

  private static void checkArguments(
      @Nullable Long startReadOffset,
      @Nullable Instant startReadTime,
      @Nullable Long stopReadOffset,
      @Nullable Instant stopReadTime) {
    checkArgument(
        startReadOffset == null || startReadTime == null,
        "startReadOffset and startReadTime are optional but mutually exclusive. Please set only one of them.");
    checkArgument(
        stopReadOffset == null || stopReadTime == null,
        "stopReadOffset and stopReadTime are optional but mutually exclusive. Please set only one of them.");
  }

  @SchemaCreate
  @SuppressWarnings("all")
  // TODO(BEAM-10677): Remove this function after AutoValueSchema is fixed.
  static KafkaSourceDescriptor create(
      String topic,
      Integer partition,
      Long start_read_offset,
      Instant start_read_time,
      Long stop_read_offset,
      Instant stop_read_time,
      List<String> bootstrap_servers) {
    checkArguments(start_read_offset, start_read_time, stop_read_offset, stop_read_time);
    return new AutoValue_KafkaSourceDescriptor(
        topic,
        partition,
        start_read_offset,
        start_read_time,
        stop_read_offset,
        stop_read_time,
        bootstrap_servers);
  }
}
