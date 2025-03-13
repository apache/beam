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

import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PayloadSerializerKafkaTable extends BeamKafkaTable {
  private final PayloadSerializer serializer;

  PayloadSerializerKafkaTable(
      Schema requiredSchema,
      String bootstrapServers,
      List<String> topics,
      PayloadSerializer serializer) {
    super(requiredSchema, bootstrapServers, topics);
    this.serializer = serializer;
  }

  @Override
  protected PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput() {
    return new PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>(
        "deserialize-kafka-rows") {
      @Override
      public PCollection<Row> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
        return input
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(record -> serializer.deserialize(record.getKV().getValue())))
            .setRowSchema(getSchema());
      }
    };
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>
      getPTransformForOutput() {
    String topic = Iterables.getOnlyElement(getTopics());
    return new PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>(
        "serialize-kafka-rows") {
      @Override
      public PCollection<ProducerRecord<byte[], byte[]>> expand(PCollection<Row> input) {
        return input.apply(
            MapElements.into(new TypeDescriptor<ProducerRecord<byte[], byte[]>>() {})
                .via(row -> new ProducerRecord<>(topic, new byte[] {}, serializer.serialize(row))));
      }
    };
  }
}
