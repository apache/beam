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

import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test for {@code BeamSortRel}. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamSortRelTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void prepare() {
    registerTable(
        "ORDER_DETAILS",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DOUBLE, "price",
                Schema.FieldType.DATETIME, "order_time")
            .addRows(
                1L,
                2,
                1.0,
                new DateTime(0),
                1L,
                1,
                2.0,
                new DateTime(1),
                2L,
                4,
                3.0,
                new DateTime(2),
                2L,
                1,
                4.0,
                new DateTime(3),
                5L,
                5,
                5.0,
                new DateTime(4),
                6L,
                6,
                6.0,
                new DateTime(5),
                7L,
                7,
                7.0,
                new DateTime(6),
                8L,
                8888,
                8.0,
                new DateTime(7),
                8L,
                999,
                9.0,
                new DateTime(8),
                10L,
                100,
                10.0,
                new DateTime(9)));
    registerTable(
        "SUB_ORDER_RAM",
        TestBoundedTable.of(
            Schema.builder()
                .addField("order_id", Schema.FieldType.INT64)
                .addField("site_id", Schema.FieldType.INT32)
                .addNullableField("price", Schema.FieldType.DOUBLE)
                .build()));

    registerTable(
        "COUNT_TABLE",
        TestBoundedTable.of(
            Schema.builder().addField("count_star", Schema.FieldType.INT64).build()));
  }

  @Test
  public void testOrderBy_basic() {
    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_id asc, site_id desc limit 4";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DOUBLE, "price")
                .addRows(1L, 2, 1.0, 1L, 1, 2.0, 2L, 4, 3.0, 2L, 1, 4.0)
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_timestamp() {
    String sql =
        "SELECT order_id, site_id, price, order_time "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_time desc limit 4";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DOUBLE, "price",
                    Schema.FieldType.DATETIME, "order_time")
                .addRows(
                    7L,
                    7,
                    7.0,
                    new DateTime(6),
                    8L,
                    8888,
                    8.0,
                    new DateTime(7),
                    8L,
                    999,
                    9.0,
                    new DateTime(8),
                    10L,
                    100,
                    10.0,
                    new DateTime(9))
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_nullsFirst() {
    Schema schema =
        Schema.builder()
            .addField("order_id", Schema.FieldType.INT64)
            .addNullableField("site_id", Schema.FieldType.INT32)
            .addField("price", Schema.FieldType.DOUBLE)
            .build();

    registerTable(
        "ORDER_DETAILS",
        TestBoundedTable.of(schema)
            .addRows(1L, 2, 1.0, 1L, null, 2.0, 2L, 1, 3.0, 2L, null, 4.0, 5L, 5, 5.0));
    registerTable("SUB_ORDER_RAM", TestBoundedTable.of(schema));

    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_id asc, site_id desc NULLS FIRST limit 4";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(schema)
                .addRows(1L, null, 2.0, 1L, 2, 1.0, 2L, null, 4.0, 2L, 1, 3.0)
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_nullsLast() {
    Schema schema =
        Schema.builder()
            .addField("order_id", Schema.FieldType.INT64)
            .addNullableField("site_id", Schema.FieldType.INT32)
            .addField("price", Schema.FieldType.DOUBLE)
            .build();

    registerTable(
        "ORDER_DETAILS",
        TestBoundedTable.of(schema)
            .addRows(1L, 2, 1.0, 1L, null, 2.0, 2L, 1, 3.0, 2L, null, 4.0, 5L, 5, 5.0));
    registerTable("SUB_ORDER_RAM", TestBoundedTable.of(schema));

    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_id asc, site_id desc NULLS LAST limit 4";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(schema)
                .addRows(1L, 2, 1.0, 1L, null, 2.0, 2L, 1, 3.0, 2L, null, 4.0)
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_with_offset2() {
    Schema schema = Schema.builder().addField("count_star", Schema.FieldType.INT64).build();

    String sql =
        "INSERT INTO COUNT_TABLE(count_star) "
            + "SELECT COUNT(*) FROM (SELECT * FROM "
            + "(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) LIMIT 3 OFFSET 1)";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows).containsInAnyOrder(TestUtils.RowsBuilder.of(schema).addRows(2L).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_with_offset() {
    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_id asc, site_id desc limit 4 offset 4";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DOUBLE, "price")
                .addRows(5L, 5, 5.0, 6L, 6, 6.0, 7L, 7, 7.0, 8L, 8888, 8.0)
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_bigFetch() {
    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_id asc, site_id desc limit 11";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DOUBLE, "price")
                .addRows(
                    1L, 2, 1.0, 1L, 1, 2.0, 2L, 4, 3.0, 2L, 1, 4.0, 5L, 5, 5.0, 6L, 6, 6.0, 7L, 7,
                    7.0, 8L, 8888, 8.0, 8L, 999, 9.0, 10L, 100, 10.0)
                .getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_exception() {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("`ORDER BY` is only supported for GlobalWindows");

    String sql =
        "INSERT INTO SUB_ORDER_RAM(order_id, site_id)  SELECT "
            + " order_id, COUNT(*) "
            + "FROM ORDER_DETAILS "
            + "GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)"
            + "ORDER BY order_id asc limit 11";

    TestPipeline pipeline = TestPipeline.create();
    compilePipeline(sql, pipeline);
  }

  @Test
  public void testNodeStatsEstimation() {
    String sql =
        "SELECT order_id, site_id, price, order_time "
            + "FROM ORDER_DETAILS "
            + "ORDER BY order_time asc limit 11";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamSortRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRate(), 0.01);

    Assert.assertEquals(10., estimate.getRowCount(), 0.01);
    Assert.assertEquals(10., estimate.getWindow(), 0.01);
  }
}
