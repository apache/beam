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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/** Tests for correlated subqueries (decorrelation). */
public class BeamSqlSubqueryTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema TABLE_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("val").addInt64Field("l").build();

  private PCollection<Row> getInput() {
    List<Row> rows =
        Arrays.asList(
            Row.withSchema(TABLE_SCHEMA).addValues(1, "row1", 1000L).build(),
            Row.withSchema(TABLE_SCHEMA).addValues(2, "row2", 2000L).build(),
            Row.withSchema(TABLE_SCHEMA).addValues(3, "row3", 3000L).build(),
            Row.withSchema(TABLE_SCHEMA).addValues(4, "row4", 4000L).build(),
            Row.withSchema(TABLE_SCHEMA).addValues(5, "row5", 1000L).build());
    return pipeline.apply(Create.of(rows).withRowSchema(TABLE_SCHEMA));
  }

  @Test
  public void testCorrelatedExistsInFilter() throws Exception {
    PCollection<Row> input = getInput();
    String sql =
        "SELECT id, val FROM PCOLLECTION a "
            + "WHERE EXISTS ( "
            + "  SELECT 1 FROM PCOLLECTION b "
            + "  WHERE b.l = a.l AND b.id > 2 "
            + ")";

    PCollection<Row> result = input.apply("testExistsInFilter", SqlTransform.query(sql));

    Schema resultSchema = Schema.builder().addInt32Field("id").addStringField("val").build();

    Row row3 = Row.withSchema(resultSchema).addValues(3, "row3").build();
    Row row4 = Row.withSchema(resultSchema).addValues(4, "row4").build();
    Row row5 = Row.withSchema(resultSchema).addValues(5, "row5").build();
    Row row1 = Row.withSchema(resultSchema).addValues(1, "row1").build();

    PAssert.that(result).containsInAnyOrder(row1, row3, row4, row5);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCorrelatedExistsWithOr() throws Exception {
    PCollection<Row> input = getInput();
    String sql =
        "SELECT id, val FROM PCOLLECTION a "
            + "WHERE id > 3 OR EXISTS ( "
            + "  SELECT 1 FROM PCOLLECTION b "
            + "  WHERE b.l = a.l AND b.id = 5 "
            + ")";

    PCollection<Row> result = input.apply("testExistsWithOr", SqlTransform.query(sql));

    Schema resultSchema = Schema.builder().addInt32Field("id").addStringField("val").build();

    Row row1 = Row.withSchema(resultSchema).addValues(1, "row1").build();
    Row row4 = Row.withSchema(resultSchema).addValues(4, "row4").build();
    Row row5 = Row.withSchema(resultSchema).addValues(5, "row5").build();

    PAssert.that(result).containsInAnyOrder(row1, row4, row5);

    pipeline.run().waitUntilFinish();
  }
}
