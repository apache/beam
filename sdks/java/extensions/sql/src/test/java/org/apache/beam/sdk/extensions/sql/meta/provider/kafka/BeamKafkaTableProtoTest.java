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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

public class BeamKafkaTableProtoTest extends BeamKafkaTableTest {

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt64Field("f_long")
          .addInt32Field("f_int")
          .addDoubleField("f_double")
          .addStringField("f_string")
          .addArrayField("f_float_array", Schema.FieldType.FLOAT)
          .build();

  private static final Schema SHUFFLED_SCHEMA =
      Schema.builder()
          .addStringField("f_string")
          .addInt32Field("f_int")
          .addArrayField("f_float_array", Schema.FieldType.FLOAT)
          .addDoubleField("f_double")
          .addInt64Field("f_long")
          .build();

  @Test
  public void testWithShuffledSchema() {
    BeamKafkaTable kafkaTable =
        new BeamKafkaProtoTable(
            SHUFFLED_SCHEMA, "", ImmutableList.of(), KafkaMessages.TestMessage.class);

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
    return new BeamKafkaProtoTable(
        TEST_SCHEMA, "", ImmutableList.of(), KafkaMessages.TestMessage.class);
  }

  @Override
  protected Row generateRow(int i) {
    List<Object> values =
        ImmutableList.of((long) i, i, (double) i, "proto_value" + i, ImmutableList.of((float) i));
    return Row.withSchema(TEST_SCHEMA).addValues(values).build();
  }

  @Override
  protected byte[] generateEncodedPayload(int i) throws IOException {
    KafkaMessages.TestMessage message =
        KafkaMessages.TestMessage.newBuilder()
            .setFLong(i)
            .setFInt(i)
            .setFDouble(i)
            .setFString("proto_value" + i)
            .addFFloatArray((float) i)
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    message.writeDelimitedTo(out);
    return out.toByteArray();
  }

  private Row shuffledRow(int i) {
    List<Object> values =
        ImmutableList.of("proto_value" + i, i, ImmutableList.of((float) i), (double) i, (long) i);
    return Row.withSchema(SHUFFLED_SCHEMA).addValues(values).build();
  }
}
