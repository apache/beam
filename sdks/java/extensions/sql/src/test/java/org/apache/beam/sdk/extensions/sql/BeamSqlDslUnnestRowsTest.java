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

import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

/** Tests for nested rows handling. */
public class BeamSqlDslUnnestRowsTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  /**
   * TODO([BEAM-14026]): This is a test of the incorrect behavior unnest because calcite flattens
   * the row.
   */
  @Test
  public void testUnnestArrayWithNestedRows() {

    Schema level3Type =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();

    Row level3Row1 = Row.withSchema(level3Type).addValues(1, "row", 1.0).build();
    Row level3Row2 = Row.withSchema(level3Type).addValues(2, "row", 2.0).build();
    Row level3Row3 = Row.withSchema(level3Type).addValues(3, "row", 3.0).build();

    // define the input row format level3
    Schema level2Type =
        Schema.builder()
            .addInt32Field("b1")
            .addStringField("b2")
            .addRowField("b3", level3Type)
            .addDoubleField("b4")
            .build();

    Row level2Row1 = Row.withSchema(level2Type).addValues(1, "row", level3Row1, 1.0).build();
    Row level2Row2 = Row.withSchema(level2Type).addValues(2, "row", level3Row2, 2.0).build();
    Row level2Row3 = Row.withSchema(level2Type).addValues(3, "row", level3Row3, 3.0).build();

    // define the input row format level3
    Schema level1Type =
        Schema.builder()
            .addInt32Field("a1")
            .addStringField("a2")
            .addDoubleField("a3")
            .addArrayField("a4", Schema.FieldType.row(level2Type))
            .build();
    Row level1Row1 =
        Row.withSchema(level1Type)
            .addValues(1, "row", 1.0, Arrays.asList(level2Row1, level2Row2, level2Row3))
            .build();
    Row level1Row2 =
        Row.withSchema(level1Type)
            .addValues(2, "row", 2.0, Arrays.asList(level2Row1, level2Row2, level2Row3))
            .build();
    Row level1Row3 =
        Row.withSchema(level1Type)
            .addValues(3, "row", 3.0, Arrays.asList(level2Row1, level2Row2, level2Row3))
            .build();

    // create a source PCollection with Create.of();
    PCollection<Row> inputTable =
        PBegin.in(pipeline)
            .apply(Create.of(level1Row1, level1Row2, level1Row3).withRowSchema(level1Type));

    String sql =
        "select t.a1, t.a2, t.a3, d.b1, d.b2, d.b4, "
            + "d.b3.c1, d.b3.c2, d.b3.c3 from test t cross join unnest(t.a4) d";
    // Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("test"), inputTable).apply(SqlTransform.query(sql));

    Schema resultSchema =
        Schema.builder()
            .addInt32Field("a1")
            .addStringField("a2")
            .addDoubleField("a3")
            .addInt32Field("b1")
            .addStringField("b2")
            .addDoubleField("b4")
            .addInt32Field("c1")
            .addStringField("c2")
            .addDoubleField("c3")
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema)
                .addValues(1, "row", 1.0, 1, "row", 1.0, 1, "row", 1.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(1, "row", 1.0, 2, "row", 2.0, 2, "row", 2.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(1, "row", 1.0, 3, "row", 3.0, 3, "row", 3.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(3, "row", 3.0, 1, "row", 1.0, 1, "row", 1.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(3, "row", 3.0, 2, "row", 2.0, 2, "row", 2.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(3, "row", 3.0, 3, "row", 3.0, 3, "row", 3.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(2, "row", 2.0, 1, "row", 1.0, 1, "row", 1.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(2, "row", 2.0, 2, "row", 2.0, 2, "row", 2.0)
                .build(),
            Row.withSchema(resultSchema)
                .addValues(2, "row", 2.0, 3, "row", 3.0, 3, "row", 3.0)
                .build());
    pipeline.run();
  }
}
