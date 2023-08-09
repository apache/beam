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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.alibaba.fastjson.JSON;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class BeamKafkaTableJsonTest extends BeamKafkaTableTest {

  private static final Schema BOOL_SCHEMA = Schema.builder().addBooleanField("nested_bool").build();

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt64Field("long")
          .addStringField("string")
          .addBooleanField("bool")
          .addArrayField("array", Schema.FieldType.STRING)
          .addRowField("row", Schema.builder().addBooleanField("nested_bool").build())
          .build();

  @Override
  protected byte[] generateEncodedPayload(int i) {
    return createJson(i).getBytes(UTF_8);
  }

  @Override
  protected Row generateRow(int i) {
    boolean bool = i % 2 == 0;
    List<Object> values =
        ImmutableList.of(
            (long) i,
            "json_value" + i,
            bool,
            ImmutableList.of("array" + i),
            Row.withSchema(BOOL_SCHEMA).attachValues(bool));
    return Row.withSchema(TEST_SCHEMA).attachValues(values);
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
                    .properties(JSON.parseObject("{ \"format\": \"json\" }"))
                    .build());
  }

  private String createJson(int i) {
    boolean bool = i % 2 == 0;
    return String.format(
        "{"
            + "  \"long\": %s,"
            + "  \"string\": \"json_value%s\","
            + "  \"bool\": %s,"
            + "  \"array\": [ \"array%s\" ],"
            + "  \"row\": { "
            + "    \"nested_bool\": %s"
            + "  }"
            + "}",
        i, i, bool, i, bool);
  }
}
