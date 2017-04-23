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
package org.apache.beam.dsls.sql.schema.kafka;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamIOType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.rel.type.RelProtoDataType;

/**
 * {@code BeamKafkaTable} represent a Kafka topic, as source or target. Need to
 * extend to convert between {@code BeamSQLRow} and {@code KV<byte[], byte[]>}.
 *
 */
public abstract class BeamKafkaTable extends BaseBeamTable implements Serializable {

  private String bootstrapServers;
  private List<String> topics;
  private Map<String, Object> configUpdates;

  protected BeamKafkaTable(RelProtoDataType protoRowType) {
    super(protoRowType);
  }

  public BeamKafkaTable(RelProtoDataType protoRowType, String bootstrapServers,
      List<String> topics) {
    super(protoRowType);
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
  }

  public BeamKafkaTable updateConsumerProperties(Map<String, Object> configUpdates) {
    this.configUpdates = configUpdates;
    return this;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  public abstract PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>>
      getPTransformForInput();

  public abstract PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput();

  @Override
  public PTransform<? super PBegin, PCollection<BeamSQLRow>> buildIOReader() {
    return new PTransform<PBegin, PCollection<BeamSQLRow>>() {

      @Override
      public PCollection<BeamSQLRow> expand(PBegin input) {
        return input.apply("read",
            KafkaIO.<byte[], byte[]>read().withBootstrapServers(bootstrapServers).withTopics(topics)
                .updateConsumerProperties(configUpdates).withKeyCoder(ByteArrayCoder.of())
                .withValueCoder(ByteArrayCoder.of()).withoutMetadata())
            .apply("in_format", getPTransformForInput());

      }
    };
  }

  @Override
  public PTransform<? super PCollection<BeamSQLRow>, PDone> buildIOWriter() {
    checkArgument(topics != null && topics.size() == 1,
        "Only one topic can be acceptable as output.");

    return new PTransform<PCollection<BeamSQLRow>, PDone>() {
      @Override
      public PDone expand(PCollection<BeamSQLRow> input) {
        return input.apply("out_reformat", getPTransformForOutput()).apply("persistent",
            KafkaIO.<byte[], byte[]>write().withBootstrapServers(bootstrapServers)
                .withTopic(topics.get(0)).withKeyCoder(ByteArrayCoder.of())
                .withValueCoder(ByteArrayCoder.of()));
      }
    };
  }

}
