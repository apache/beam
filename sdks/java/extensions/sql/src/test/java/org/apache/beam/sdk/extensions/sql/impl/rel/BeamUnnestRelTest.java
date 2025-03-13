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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@code BeamUnnestRel}. */
public class BeamUnnestRelTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    Schema rowSchema =
        Schema.builder().addStringField("stringField").addInt32Field("intField").build();
    List<Row> nestedRows =
        Arrays.asList(
            Row.withSchema(rowSchema).addValues("test1", 1).build(),
            Row.withSchema(rowSchema).addValues("test2", 2).build());
    registerTable(
        "NESTED",
        TestBoundedTable.of(
                Schema.FieldType.STRING,
                "user_id",
                Schema.FieldType.array(Schema.FieldType.row(rowSchema)),
                "nested")
            .addRows("1", nestedRows));
  }

  @Test
  public void testUnnest() {
    String sql =
        "SELECT user_id, p.intField, p.stringField FROM NESTED as t, unnest(t.nested) as p";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.STRING, "user_id",
                    Schema.FieldType.INT32, "intField",
                    Schema.FieldType.STRING, "stringField")
                .addRows("1", 1, "test1", "1", 2, "test2")
                .getRows());
    pipeline.run();
  }
}
