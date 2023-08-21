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

import com.alibaba.fastjson.JSON;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class BeamKafkaTableAvroTest extends BeamKafkaTableTest {
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt64Field("f_long")
          .addInt32Field("f_int")
          .addDoubleField("f_double")
          .addStringField("f_string")
          .addBooleanField("f_bool")
          .addRowField("f_row", EMPTY_SCHEMA)
          .addArrayField("f_array", Schema.FieldType.row(EMPTY_SCHEMA))
          .build();

  private static final org.apache.avro.Schema EMPTY_AVRO_SCHEMA =
      AvroUtils.toAvroSchema(EMPTY_SCHEMA);
  private static final org.apache.avro.Schema AVRO_SCHEMA = AvroUtils.toAvroSchema(TEST_SCHEMA);
  private static final AvroCoder<GenericRecord> AVRO_CODER = AvroCoder.of(AVRO_SCHEMA);

  @Override
  protected Row generateRow(int i) {
    List<Object> values =
        ImmutableList.of(
            (long) i,
            i,
            (double) i,
            "avro_value" + i,
            i % 2 == 0,
            Row.withSchema(EMPTY_SCHEMA).build(),
            ImmutableList.of(Row.withSchema(EMPTY_SCHEMA).build()));
    return Row.withSchema(TEST_SCHEMA).addValues(values).build();
  }

  @Override
  protected byte[] generateEncodedPayload(int i) {
    GenericRecord record =
        new GenericRecordBuilder(AVRO_SCHEMA)
            .set("f_long", (long) i)
            .set("f_int", i)
            .set("f_double", (double) i)
            .set("f_string", "avro_value" + i)
            .set("f_bool", i % 2 == 0)
            .set("f_row", new GenericRecordBuilder(EMPTY_AVRO_SCHEMA).build())
            .set("f_array", ImmutableList.of(new GenericRecordBuilder(EMPTY_AVRO_SCHEMA).build()))
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      AVRO_CODER.encode(record, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return (BeamKafkaTable)
        new KafkaTableProvider()
            .buildBeamSqlTable(
                Table.builder()
                    .name("kafka")
                    .type("kafka")
                    .schema(TEST_SCHEMA)
                    .location("localhost/mytopic")
                    .properties(JSON.parseObject("{ \"format\": \"avro\" }"))
                    .build());
  }
}
