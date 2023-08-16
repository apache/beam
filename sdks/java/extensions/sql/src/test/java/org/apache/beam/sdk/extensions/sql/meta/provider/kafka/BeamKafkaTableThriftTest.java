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
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.io.thrift.payloads.TestThriftMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Test;

public class BeamKafkaTableThriftTest extends BeamKafkaTableTest {
  private final TProtocolFactory protocolFactory = new TCompactProtocol.Factory();

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt64Field("f_long")
          .addInt32Field("f_int")
          .addDoubleField("f_double")
          .addStringField("f_string")
          .addArrayField("f_double_array", Schema.FieldType.DOUBLE)
          .build();

  private static final Schema SHUFFLED_SCHEMA =
      Schema.builder()
          .addStringField("f_string")
          .addInt32Field("f_int")
          .addArrayField("f_double_array", Schema.FieldType.DOUBLE)
          .addDoubleField("f_double")
          .addInt64Field("f_long")
          .build();

  @Test
  public void testWithShuffledSchema() {
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
        containsString("does not match schema inferred from thrift class.\nThrift class: "));
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
                            "{ \"format\": \"thrift\", \"thriftClass\": \""
                                + TestThriftMessage.class.getName()
                                + "\", \"thriftProtocolFactoryClass\": \""
                                + TCompactProtocol.Factory.class.getName()
                                + "\" }"))
                    .build());
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return getBeamKafkaTable(TEST_SCHEMA);
  }

  @Override
  protected Row generateRow(int i) {
    return Row.withSchema(TEST_SCHEMA)
        .addValues((long) i, i, (double) i, "thrift_value" + i, ImmutableList.of((double) i))
        .build();
  }

  @Override
  protected byte[] generateEncodedPayload(int i) {
    final TestThriftMessage message =
        new TestThriftMessage().setFLong(i).setFInt(i).setFDouble(i).setFString("thrift_value" + i);
    message.addToFDoubleArray(i);

    try {
      return new TSerializer(protocolFactory).serialize(message);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private Row shuffledRow(int i) {
    List<Object> values =
        ImmutableList.of("thrift_value" + i, i, ImmutableList.of((double) i), (double) i, (long) i);
    return Row.withSchema(SHUFFLED_SCHEMA).addValues(values).build();
  }
}
