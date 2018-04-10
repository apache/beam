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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * {@code BeamKafkaTable} represent a Kafka topic, as source or target. Need to
 * extend to convert between {@code BeamSqlRow} and {@code KV<byte[], byte[]>}.
 *
 */
public abstract class BeamKafkaTable extends BaseBeamTable implements Serializable {
  private String bootstrapServers;
  private List<String> topics;
  private List<TopicPartition> topicPartitions;
  private Map<String, Object> configUpdates;

  protected BeamKafkaTable(Schema beamSchema) {
    super(beamSchema);
  }

  public BeamKafkaTable(Schema beamSchema, String bootstrapServers,
                        List<String> topics) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
  }

  public BeamKafkaTable(Schema beamSchema,
      List<TopicPartition> topicPartitions, String bootstrapServers) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topicPartitions = topicPartitions;
  }

  public BeamKafkaTable updateConsumerProperties(Map<String, Object> configUpdates) {
    this.configUpdates = configUpdates;
    return this;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  public abstract PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput();

  public abstract PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput();

  @Override
  public PCollection<Row> buildIOReader(Pipeline pipeline) {
    KafkaIO.Read<byte[], byte[]> kafkaRead = null;
    if (topics != null) {
      kafkaRead = KafkaIO.<byte[], byte[]>read()
      .withBootstrapServers(bootstrapServers)
      .withTopics(topics)
      .updateConsumerProperties(configUpdates)
      .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
      .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else if (topicPartitions != null) {
      kafkaRead = KafkaIO.<byte[], byte[]>read()
          .withBootstrapServers(bootstrapServers)
          .withTopicPartitions(topicPartitions)
          .updateConsumerProperties(configUpdates)
          .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
          .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else {
      throw new IllegalArgumentException("One of topics and topicPartitions must be configurated.");
    }

    return PBegin.in(pipeline).apply("read", kafkaRead.withoutMetadata())
.apply("in_format", getPTransformForInput());
  }

  @Override
  public PTransform<? super PCollection<Row>, PDone> buildIOWriter() {
    checkArgument(topics != null && topics.size() == 1,
        "Only one topic can be acceptable as output.");

    return new PTransform<PCollection<Row>, PDone>() {
      @Override
      public PDone expand(PCollection<Row> input) {
        return input.apply("out_reformat", getPTransformForOutput()).apply("persistent",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(topics.get(0))
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
      }
    };
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public List<String> getTopics() {
    return topics;
  }
}
