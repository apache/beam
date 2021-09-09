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
package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for SQL MAP type. */
public class BeamSqlMapTest {

  private static final Schema INPUT_ROW_TYPE =
      Schema.builder()
          .addInt32Field("f_int")
          .addMapField("f_intStringMap", Schema.FieldType.STRING, Schema.FieldType.INT32)
          .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectAll() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addNullableField(
                "f_map", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32))
            .build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery",
            SqlTransform.query("SELECT f_int, f_intStringMap as f_map FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType)
                .addValues(1, ImmutableMap.of("key11", 11, "key22", 22))
                .build(),
            Row.withSchema(resultType)
                .addValues(2, ImmutableMap.of("key33", 33, "key44", 44, "key55", 55))
                .build());

    pipeline.run();
  }

  @Test
  public void testSelectMapField() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addMapField("f_intStringMap", Schema.FieldType.STRING, Schema.FieldType.INT32)
            .build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery", SqlTransform.query("SELECT 42, MAP['aa', 1] as `f_map` FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(42, ImmutableMap.of("aa", 1)).build(),
            Row.withSchema(resultType).addValues(42, ImmutableMap.of("aa", 1)).build());

    pipeline.run();
  }

  @Test
  public void testSelectMapFieldKeyValueSameType() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addMapField("f_intStringMap", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery",
            SqlTransform.query("SELECT 42, MAP['aa', '1'] as `f_map` FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(42, ImmutableMap.of("aa", "1")).build(),
            Row.withSchema(resultType).addValues(42, ImmutableMap.of("aa", "1")).build());

    pipeline.run();
  }

  @Test
  public void testAccessMapElement() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder().addNullableField("f_mapElem", Schema.FieldType.INT32).build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery", SqlTransform.query("SELECT f_intStringMap['key11'] FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(11).build(),
            Row.withSchema(resultType).addValue(null).build());

    pipeline.run();
  }

  private PCollection<Row> pCollectionOf2Elements() {
    return pipeline.apply(
        "boundedInput1",
        Create.of(
                Row.withSchema(INPUT_ROW_TYPE)
                    .addValues(1)
                    .addValue(ImmutableMap.of("key11", 11, "key22", 22))
                    .build(),
                Row.withSchema(INPUT_ROW_TYPE)
                    .addValues(2)
                    .addValue(ImmutableMap.of("key33", 33, "key44", 44, "key55", 55))
                    .build())
            .withRowSchema(INPUT_ROW_TYPE));
  }
}
