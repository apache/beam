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

import java.util.HashMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for SQL MAP type.
 */
public class BeamSqlMapTest {

  private static final Schema INPUT_ROW_TYPE = RowSqlTypes.builder().withIntegerField("f_int")
      .withMapField("f_intStringMap", SqlTypeName.VARCHAR, SqlTypeName.INTEGER).build();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  @Rule
  public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectAll() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType = RowSqlTypes.builder().withIntegerField("f_int")
        .withMapField("f_map", SqlTypeName.VARCHAR, SqlTypeName.INTEGER).build();

    PCollection<Row> result = input.apply("sqlQuery",
        BeamSql.query("SELECT f_int, f_intStringMap as f_map FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(Row.withSchema(resultType).addValues(1, new HashMap<String, Integer>() {
          {
            put("key11", 11);
            put("key22", 22);
          }
        }).build(),

            Row.withSchema(resultType).addValues(2, new HashMap<String, Integer>() {
              {
                put("key33", 33);
                put("key44", 44);
                put("key55", 55);
              }
            }).build());

    pipeline.run();
  }

  @Test
  public void testSelectMapField() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType = RowSqlTypes.builder().withIntegerField("f_int")
        .withMapField("f_intStringMap", SqlTypeName.VARCHAR, SqlTypeName.INTEGER).build();

    PCollection<Row> result = input.apply("sqlQuery",
        BeamSql.query("SELECT 42, MAP['aa', 1] as `f_map` FROM PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(
        Row.withSchema(resultType).addValues(42, new HashMap<String, Integer>() {
          {
            put("aa", 1);
          }
        }).build(), Row.withSchema(resultType).addValues(42, new HashMap<String, Integer>() {
          {
            put("aa", 1);
          }
        }).build());

    pipeline.run();
  }

  @Test
  public void testAccessMapElement() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType = RowSqlTypes.builder().withIntegerField("f_mapElem").build();

    PCollection<Row> result = input.apply("sqlQuery",
        BeamSql.query("SELECT f_intStringMap['key11'] FROM PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(Row.withSchema(resultType).addValues(11).build(),
        Row.withSchema(resultType).addValue(null).build());

    pipeline.run();
  }

  private PCollection<Row> pCollectionOf2Elements() {
    return PBegin.in(pipeline).apply("boundedInput1", Create
        .of(Row.withSchema(INPUT_ROW_TYPE).addValues(1).addValue(new HashMap<String, Integer>() {
          {
            put("key11", 11);
            put("key22", 22);
          }
        }).build(),
            Row.withSchema(INPUT_ROW_TYPE).addValues(2).addValue(new HashMap<String, Integer>() {
              {
                put("key33", 33);
                put("key44", 44);
                put("key55", 55);
              }
            }).build())
        .withCoder(INPUT_ROW_TYPE.getRowCoder()));
  }
}
