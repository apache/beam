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

import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link SchemaTranslation}. */
public class SchemaTranslationTest {

  /** Tests round-trip proto encodings for {@link Schema}. */
  @RunWith(Parameterized.class)
  public static class ToFromProtoTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Schema> data() {
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
          .add(Schema.of(Field.of("logical", FieldType.logicalType(FixedBytes.of(24)))))
          .build();
    }

    @Parameter(0)
    public Schema schema;

    @Test
    public void toAndFromProto() throws Exception {
      SchemaApi.Schema schemaProto = SchemaTranslation.schemaToProto(schema, true);

      Schema decodedSchema = SchemaTranslation.fromProto(schemaProto);
      assertThat(decodedSchema, equalTo(schema));
    }
  }
}
