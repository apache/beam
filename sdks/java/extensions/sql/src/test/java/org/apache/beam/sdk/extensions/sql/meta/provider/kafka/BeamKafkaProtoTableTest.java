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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class BeamKafkaProtoTableTest extends BeamKafkaTableTest {
  private final SerializableFunction<Row, KafkaMessages.TestMessage> toMessageFn =
      new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(KafkaMessages.TestMessage.class));

  private final ProtoCoder<KafkaMessages.TestMessage> coder =
      ProtoCoder.of(KafkaMessages.TestMessage.class);

  @Override
  protected Schema getSchema() {
    return Schema.builder()
        .addNullableField("f_long", Schema.FieldType.INT64)
        .addNullableField("f_int", Schema.FieldType.INT32)
        .addNullableField("f_double", Schema.FieldType.DOUBLE)
        .addNullableField("f_string", Schema.FieldType.STRING)
        .addNullableField("f_float_array", Schema.FieldType.array(Schema.FieldType.FLOAT))
        .build();
  }

  @Override
  protected List<Object> listFrom(int i) {
    return ImmutableList.of((long) i, i, (double) i, "value" + i, ImmutableList.of((float) i));
  }

  @Override
  protected KafkaTestTable getTestTable(int numberOfPartitions) {
    return new KafkaTestTableProto(getSchema(), TOPICS, numberOfPartitions);
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return new BeamKafkaProtoTable<>(
        getSchema(), "", ImmutableList.of(), KafkaMessages.TestMessage.class);
  }

  @Override
  protected KafkaTestRecord<?> createKafkaTestRecord(String key, int i, long timestamp) {
    Row row = generateRow(i);
    KafkaMessages.TestMessage message = toMessageFn.apply(row);
    byte[] protoBytes = encodeMessageToBytes(message);
    return KafkaTestRecord.create(key, protoBytes, "topic1", timestamp);
  }

  @Override
  protected PCollection<KV<byte[], byte[]>> applyRowToBytesKV(PCollection<Row> rows) {
    return rows.apply(
            "mapRowToMessage",
            MapElements.into(TypeDescriptor.of(KafkaMessages.TestMessage.class)).via(toMessageFn))
        .setCoder(ProtoCoder.of(KafkaMessages.TestMessage.class))
        .apply(
            "encodeMessage",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(byte[].class), TypeDescriptor.of(byte[].class)))
                .via(
                    message -> {
                      ProtoCoder<KafkaMessages.TestMessage> coder =
                          ProtoCoder.of(KafkaMessages.TestMessage.class);
                      ByteArrayOutputStream out = new ByteArrayOutputStream();
                      try {
                        coder.encode(message, out);
                      } catch (IOException e) {
                        throw new RuntimeException("Encode failed.", e);
                      }
                      return KV.of(new byte[] {}, out.toByteArray());
                    }));
  }

  private byte[] encodeMessageToBytes(KafkaMessages.TestMessage message) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      coder.encode(message, out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Encode failed.", e);
    }
  }
}
