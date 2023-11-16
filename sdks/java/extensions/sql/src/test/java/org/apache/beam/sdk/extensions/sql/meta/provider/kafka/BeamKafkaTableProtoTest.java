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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.alibaba.fastjson.JSON;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.protobuf.PayloadMessages;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
  public void testWithShuffledSchema() throws Exception {
    BeamKafkaTable kafkaTable = getBeamKafkaTable(SHUFFLED_SCHEMA);

    PCollection<Row> result =
        pipeline
            .apply(Create.of(shuffledRow(1), shuffledRow(2)))
            .apply(kafkaTable.getPTransformForOutput())
            .setCoder(ProducerRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
            .apply(MapElements.via(new ProducerToRecord()))
            .setCoder(KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
            .apply(kafkaTable.getPTransformForInput());
    PAssert.that(result).containsInAnyOrder(shuffledRow(1), shuffledRow(2));
    pipeline.run();
  }

  @Test
  public void testSchemasDoNotMatch() {
    Schema schema = Schema.builder().addStringField("non_existing_field").build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> getBeamKafkaTable(schema));

    assertThat(
        e.getMessage(),
        containsString("does not match schema inferred from protobuf class.\nProtobuf class: "));
  }

  private static BeamKafkaTable getBeamKafkaTable(Schema schema) {
    return (BeamKafkaTable)
        new KafkaTableProvider()
            .buildBeamSqlTable(
                Table.builder()
                    .name("kafka")
                    .type("kafka")
                    .schema(schema)
                    .location("localhost/mytopic")
                    .properties(
                        JSON.parseObject(
                            "{ \"format\": \"proto\", \"protoClass\": \""
                                + PayloadMessages.TestMessage.class.getName()
                                + "\" }"))
                    .build());
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return getBeamKafkaTable(TEST_SCHEMA);
  }

  @Override
  protected Row generateRow(int i) {
    List<Object> values =
        ImmutableList.of((long) i, i, (double) i, "proto_value" + i, ImmutableList.of((float) i));
    return Row.withSchema(TEST_SCHEMA).addValues(values).build();
  }

  @Override
  protected byte[] generateEncodedPayload(int i) {
    PayloadMessages.TestMessage message =
        PayloadMessages.TestMessage.newBuilder()
            .setFLong(i)
            .setFInt(i)
            .setFDouble(i)
            .setFString("proto_value" + i)
            .addFFloatArray((float) i)
            .build();

    return message.toByteArray();
  }

  private Row shuffledRow(int i) {
    List<Object> values =
        ImmutableList.of("proto_value" + i, i, ImmutableList.of((float) i), (double) i, (long) i);
    return Row.withSchema(SHUFFLED_SCHEMA).addValues(values).build();
  }
}
