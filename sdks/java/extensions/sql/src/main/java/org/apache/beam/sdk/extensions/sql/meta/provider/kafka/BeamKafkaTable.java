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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * {@code BeamKafkaTable} represent a Kafka topic, as source or target. Need to extend to convert
 * between {@code BeamSqlRow} and {@code KV<byte[], byte[]>}.
 */
public abstract class BeamKafkaTable extends BaseBeamTable {
  private String bootstrapServers;
  private List<String> topics;
  private List<TopicPartition> topicPartitions;
  private Map<String, Object> configUpdates;

  protected BeamKafkaTable(Schema beamSchema) {
    super(beamSchema);
  }

  public BeamKafkaTable(Schema beamSchema, String bootstrapServers, List<String> topics) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.configUpdates = new HashMap<>();
  }

  public BeamKafkaTable(
      Schema beamSchema, List<TopicPartition> topicPartitions, String bootstrapServers) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topicPartitions = topicPartitions;
  }

  public BeamKafkaTable updateConsumerProperties(Map<String, Object> configUpdates) {
    this.configUpdates = configUpdates;
    return this;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  public abstract PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput();

  public abstract PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput();

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    KafkaIO.Read<byte[], byte[]> kafkaRead = null;
    if (topics != null) {
      kafkaRead =
          KafkaIO.<byte[], byte[]>read()
              .withBootstrapServers(bootstrapServers)
              .withTopics(topics)
              .withConsumerConfigUpdates(configUpdates)
              .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
              .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else if (topicPartitions != null) {
      kafkaRead =
          KafkaIO.<byte[], byte[]>read()
              .withBootstrapServers(bootstrapServers)
              .withTopicPartitions(topicPartitions)
              .withConsumerConfigUpdates(configUpdates)
              .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
              .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else {
      throw new IllegalArgumentException("One of topics and topicPartitions must be configurated.");
    }

    return begin
        .apply("read", kafkaRead.withoutMetadata())
        .apply("in_format", getPTransformForInput())
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    checkArgument(
        topics != null && topics.size() == 1, "Only one topic can be acceptable as output.");
    assert topics != null;

    return input
        .apply("out_reformat", getPTransformForOutput())
        .apply(
            "persistent",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(topics.get(0))
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public List<String> getTopics() {
    return topics;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }
}
