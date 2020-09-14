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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link SchemaTranslation}. */
@RunWith(Enclosed.class)
public class SchemaTranslationTest {

  /** Tests round-trip proto encodings for {@link Schema}. */
  @RunWith(Parameterized.class)
  public static class ToFromProtoTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Schema> data() {
      Map<String, Integer> optionMap = new HashMap<>();
      optionMap.put("string", 42);
      List<String> optionList = new ArrayList<>();
      optionList.add("string");
      Row optionRow =
          Row.withSchema(
                  Schema.builder()
                      .addField("field_one", FieldType.STRING)
                      .addField("field_two", FieldType.INT32)
                      .build())
              .addValue("value")
              .addValue(42)
              .build();

      Schema.Options.Builder optionsBuilder =
          Schema.Options.builder()
              .setOption("field_option_boolean", FieldType.BOOLEAN, true)
              .setOption("field_option_byte", FieldType.BYTE, (byte) 12)
              .setOption("field_option_int16", FieldType.INT16, (short) 12)
              .setOption("field_option_int32", FieldType.INT32, 12)
              .setOption("field_option_int64", FieldType.INT64, 12L)
              .setOption("field_option_string", FieldType.STRING, "foo")
              .setOption("field_option_bytes", FieldType.BYTES, new byte[] {0x42, 0x69, 0x00})
              .setOption("field_option_float", FieldType.FLOAT, (float) 12.0)
              .setOption("field_option_double", FieldType.DOUBLE, 12.0)
              .setOption(
                  "field_option_map", FieldType.map(FieldType.STRING, FieldType.INT32), optionMap)
              .setOption("field_option_array", FieldType.array(FieldType.STRING), optionList)
              .setOption("field_option_row", optionRow)
              .setOption("field_option_value", FieldType.STRING, "other");

      return ImmutableList.<Schema>builder()
          .add(Schema.of(Field.of("string", FieldType.STRING)))
          .add(
              Schema.of(
                  Field.of("boolean", FieldType.BOOLEAN),
                  Field.of("byte", FieldType.BYTE),
                  Field.of("int16", FieldType.INT16),
                  Field.of("int32", FieldType.INT32),
                  Field.of("int64", FieldType.INT64)))
          .add(
              Schema.of(
                  Field.of(
                      "row",
                      FieldType.row(
                          Schema.of(
                              Field.of("foo", FieldType.STRING),
                              Field.of("bar", FieldType.DOUBLE),
                              Field.of("baz", FieldType.BOOLEAN))))))
          .add(
              Schema.of(
                  Field.of(
                      "array(array(int64)))",
                      FieldType.array(FieldType.array(FieldType.INT64.withNullable(true))))))
          .add(
              Schema.of(
                  Field.of(
                      "iter(iter(int64)))",
                      FieldType.iterable(FieldType.iterable(FieldType.INT64.withNullable(true))))))
          .add(
              Schema.of(
                  Field.of("nullable", FieldType.STRING.withNullable(true)),
                  Field.of("non_nullable", FieldType.STRING.withNullable(false))))
          .add(
              Schema.of(
                  Field.of("decimal", FieldType.DECIMAL), Field.of("datetime", FieldType.DATETIME)))
          .add(Schema.of(Field.of("fixed_bytes", FieldType.logicalType(FixedBytes.of(24)))))
          .add(Schema.of(Field.of("micros_instant", FieldType.logicalType(new MicrosInstant()))))
          .add(
              Schema.of(
                      Field.of("field_with_option_atomic", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_atomic", FieldType.INT32, Integer.valueOf(42))
                                  .build()))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption("schema_option_atomic", FieldType.BOOLEAN, true)))
          .add(
              Schema.of(
                      Field.of("field_with_option_map", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_map",
                                      FieldType.map(FieldType.STRING, FieldType.INT32),
                                      optionMap)))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption(
                              "field_option_map",
                              FieldType.map(FieldType.STRING, FieldType.INT32),
                              optionMap)))
          .add(
              Schema.of(
                      Field.of("field_with_option_array", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_array",
                                      FieldType.array(FieldType.STRING),
                                      optionList)
                                  .build()))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption(
                              "field_option_array", FieldType.array(FieldType.STRING), optionList)))
          .add(
              Schema.of(Field.of("field", FieldType.STRING).withOptions(optionsBuilder))
                  .withOptions(optionsBuilder))
          .build();
    }

    @Parameter(0)
    public Schema schema;

    @Test
    public void toAndFromProto() throws Exception {
      SchemaApi.Schema schemaProto = SchemaTranslation.schemaToProto(schema, true);

      Schema decodedSchema = SchemaTranslation.schemaFromProto(schemaProto);
      assertThat(decodedSchema, equalTo(schema));
    }
  }
}
