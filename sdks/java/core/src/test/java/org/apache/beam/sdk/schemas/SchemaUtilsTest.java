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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;

/** Tests for {@link org.apache.beam.sdk.schemas.SchemaUtils}. */
public class SchemaUtilsTest {
  @Test
  public void testWidenPrimitives() {
    Schema schema1 =
        Schema.builder()
            .addField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema schema2 =
        Schema.builder()
            .addNullableField("field3", FieldType.INT32)
            .addField("field4", FieldType.STRING)
            .build();
    Schema expected =
        Schema.builder()
            .addNullableField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenNested() {
    Schema schema1 =
        Schema.builder()
            .addField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema schema2 =
        Schema.builder()
            .addNullableField("field3", FieldType.INT32)
            .addField("field4", FieldType.STRING)
            .build();
    Schema top1 = Schema.builder().addField("top1", FieldType.row(schema1)).build();
    Schema top2 = Schema.builder().addField("top2", FieldType.row(schema2)).build();
    Schema expected =
        Schema.builder()
            .addNullableField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema expectedTop = Schema.builder().addField("top1", FieldType.row(expected)).build();

    assertEquals(expectedTop, SchemaUtils.mergeWideningNullable(top1, top2));
  }

  @Test
  public void testWidenArray() {
    Schema schema1 = Schema.builder().addArrayField("field1", FieldType.INT32).build();
    Schema schema2 =
        Schema.builder().addArrayField("field1", FieldType.INT32.withNullable(true)).build();
    Schema expected =
        Schema.builder().addArrayField("field1", FieldType.INT32.withNullable(true)).build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenIterable() {
    Schema schema1 = Schema.builder().addIterableField("field1", FieldType.INT32).build();
    Schema schema2 =
        Schema.builder().addIterableField("field1", FieldType.INT32.withNullable(true)).build();
    Schema expected =
        Schema.builder().addIterableField("field1", FieldType.INT32.withNullable(true)).build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenMap() {
    Schema schema1 =
        Schema.builder().addMapField("field1", FieldType.INT32, FieldType.INT32).build();
    Schema schema2 =
        Schema.builder()
            .addMapField(
                "field1", FieldType.INT32.withNullable(true), FieldType.INT32.withNullable(true))
            .build();
    Schema expected =
        Schema.builder()
            .addMapField(
                "field1", FieldType.INT32.withNullable(true), FieldType.INT32.withNullable(true))
            .build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }
}
