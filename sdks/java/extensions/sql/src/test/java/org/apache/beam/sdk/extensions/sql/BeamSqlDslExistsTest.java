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

import org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Tests for comparison operator in queries. */
public class BeamSqlDslExistsTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    registerTable(
        "CUSTOMER",
        TestBoundedTable.of(
                Schema.FieldType.INT32, "c_custkey",
                Schema.FieldType.DOUBLE, "c_acctbal",
                Schema.FieldType.STRING, "c_city")
            .addRows(1, 1.0, "Seattle", 5, 1.0, "Mountain View", 4, 4.0, "Portland"));

    registerTable(
        "ORDERS",
        TestBoundedTable.of(
                Schema.FieldType.INT32, "o_orderkey",
                Schema.FieldType.INT32, "o_custkey",
                Schema.FieldType.DOUBLE, "o_totalprice")
            .addRows(1, 1, 1.0, 2, 2, 2.0, 3, 3, 3.0));
  }

  @Test
  public void testExistsSubquery() {
    String sql =
        "select * from CUSTOMER "
            + " where exists ( "
            + " select * from ORDERS "
            + " where o_custkey = c_custkey )";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "c_custkey",
                    Schema.FieldType.DOUBLE, "c_acctbal",
                    Schema.FieldType.STRING, "c_city")
                .addRows(1, 1.0, "Seattle")
                .getRows());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNotExistsSubquery() {
    String sql =
        "select * from CUSTOMER "
            + " where not exists ( "
            + " select * from ORDERS "
            + " where o_custkey = c_custkey )";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "c_custkey",
                    Schema.FieldType.DOUBLE, "c_acctbal",
                    Schema.FieldType.STRING, "c_city")
                .addRows(4, 4.0, "Portland", 5, 1.0, "Mountain View")
                .getRows());

    pipeline.run().waitUntilFinish();
  }
}
