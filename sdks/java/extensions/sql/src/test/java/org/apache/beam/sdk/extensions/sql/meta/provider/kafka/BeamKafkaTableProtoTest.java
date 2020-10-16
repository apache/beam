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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

public class BeamKafkaTableProtoTest extends BeamKafkaTableTest {
  private final ProtoCoder<KafkaMessages.TestMessage> PROTO_CODER =
      ProtoCoder.of(KafkaMessages.TestMessage.class);

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addNullableField("f_long", Schema.FieldType.INT64)
          .addNullableField("f_int", Schema.FieldType.INT32)
          .addNullableField("f_double", Schema.FieldType.DOUBLE)
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_float_array", Schema.FieldType.array(Schema.FieldType.FLOAT))
          .build();

  private static final Schema SHUFFLED_SCHEMA =
      Schema.builder()
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_int", Schema.FieldType.INT32)
          .addNullableField("f_float_array", Schema.FieldType.array(Schema.FieldType.FLOAT))
          .addNullableField("f_double", Schema.FieldType.DOUBLE)
          .addNullableField("f_long", Schema.FieldType.INT64)
          .build();

  @Test
  public void testWithShuffledSchema() {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();
    PCollection<Row> result =
        pipeline
            .apply(Create.of(shuffledRow(1), shuffledRow(2)))
            .apply(kafkaTable.getPTransformForOutput())
            .apply(kafkaTable.getPTransformForInput());
    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return new BeamKafkaProtoTable<>("", ImmutableList.of(), KafkaMessages.TestMessage.class);
  }

  @Override
  protected Row generateRow(int i) {
    List<Object> values =
        ImmutableList.of((long) i, i, (double) i, "proto_value" + i, ImmutableList.of((float) i));
    return Row.withSchema(TEST_SCHEMA).addValues(values).build();
  }

  @Override
  protected byte[] generateEncodedPayload(int i) {
    KafkaMessages.TestMessage message =
        KafkaMessages.TestMessage.newBuilder()
            .setFLong(i)
            .setFInt(i)
            .setFDouble(i)
            .setFString("proto_value" + i)
            .addFFloatArray((float) i)
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      PROTO_CODER.encode(message, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private Row shuffledRow(int i) {
    List<Object> values =
        ImmutableList.of("proto_value" + i, i, ImmutableList.of((float) i), (double) i, (long) i);
    return Row.withSchema(SHUFFLED_SCHEMA).addValues(values).build();
  }
}
