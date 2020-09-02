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
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Bounded + Bounded Test for {@code BeamCoGBKJoinRel}. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamCoGBKJoinRelBoundedVsBoundedTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  public static final TestBoundedTable ORDER_DETAILS1 =
      TestBoundedTable.of(
              Schema.FieldType.INT32, "order_id",
              Schema.FieldType.INT32, "site_id",
              Schema.FieldType.INT32, "price")
          .addRows(1, 2, 3, 2, 3, 3, 3, 4, 5);

  public static final TestBoundedTable ORDER_DETAILS2 =
      TestBoundedTable.of(
              Schema.FieldType.INT32, "order_id",
              Schema.FieldType.INT32, "site_id",
              Schema.FieldType.INT32, "price")
          .addRows(1, 2, 3, 2, 3, 3, 3, 4, 5);

  @BeforeClass
  public static void prepare() {
    registerTable("ORDER_DETAILS1", ORDER_DETAILS1);
    registerTable("ORDER_DETAILS2", ORDER_DETAILS2);
  }

  @Test
  public void testInnerJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("site_id", Schema.FieldType.INT32)
                        .addField("price", Schema.FieldType.INT32)
                        .addField("order_id0", Schema.FieldType.INT32)
                        .addField("site_id0", Schema.FieldType.INT32)
                        .addField("price0", Schema.FieldType.INT32)
                        .build())
                .addRows(2, 3, 3, 1, 2, 3)
                .getRows());
    pipeline.run();
  }

  @Test
  public void testNodeStatsEstimation() {
    String sql =
        "SELECT *  "
            + " FROM ORDER_DETAILS1 o1 "
            + " JOIN ORDER_DETAILS2 o2 "
            + " on "
            + " o1.order_id=o2.site_id ";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamCoGBKJoinRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());
    NodeStats leftEstimate =
        BeamSqlRelUtils.getNodeStats(
            ((BeamCoGBKJoinRel) root).getLeft(), root.getCluster().getMetadataQuery());
    NodeStats rightEstimate =
        BeamSqlRelUtils.getNodeStats(
            ((BeamCoGBKJoinRel) root).getRight(), root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRate(), 0.01);

    Assert.assertNotEquals(0d, estimate.getRowCount(), 0.001);
    Assert.assertTrue(
        estimate.getRowCount() < leftEstimate.getRowCount() * rightEstimate.getRowCount());

    Assert.assertNotEquals(0d, estimate.getWindow(), 0.001);
    Assert.assertTrue(estimate.getWindow() < leftEstimate.getWindow() * rightEstimate.getWindow());
  }

  @Test
  public void testNodeStatsOfMoreConditions() {
    String sql1 =
        "SELECT *  "
            + " FROM ORDER_DETAILS1 o1 "
            + " JOIN ORDER_DETAILS2 o2 "
            + " on "
            + " o1.order_id=o2.site_id ";

    String sql2 =
        "SELECT *  "
            + " FROM ORDER_DETAILS1 o1 "
            + " JOIN ORDER_DETAILS2 o2 "
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    RelNode root1 = env.parseQuery(sql1);

    while (!(root1 instanceof BeamCoGBKJoinRel)) {
      root1 = root1.getInput(0);
    }

    RelNode root2 = env.parseQuery(sql2);

    while (!(root2 instanceof BeamCoGBKJoinRel)) {
      root2 = root2.getInput(0);
    }

    NodeStats estimate1 =
        BeamSqlRelUtils.getNodeStats(root1, root1.getCluster().getMetadataQuery());
    NodeStats estimate2 =
        BeamSqlRelUtils.getNodeStats(root2, root1.getCluster().getMetadataQuery());

    Assert.assertNotEquals(0d, estimate2.getRowCount(), 0.001);
    // A join with two conditions should have lower estimate.
    Assert.assertTrue(estimate2.getRowCount() < estimate1.getRowCount());
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " LEFT OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("site_id", Schema.FieldType.INT32)
                        .addField("price", Schema.FieldType.INT32)
                        .addNullableField("order_id0", Schema.FieldType.INT32)
                        .addNullableField("site_id0", Schema.FieldType.INT32)
                        .addNullableField("price0", Schema.FieldType.INT32)
                        .build())
                .addRows(1, 2, 3, null, null, null, 2, 3, 3, 1, 2, 3, 3, 4, 5, null, null, null)
                .getRows());
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoinWithEmptyTuplesOnRightSide() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " LEFT OUTER JOIN (SELECT * FROM ORDER_DETAILS2 WHERE FALSE) o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("site_id", Schema.FieldType.INT32)
                        .addField("price", Schema.FieldType.INT32)
                        .addNullableField("order_id0", Schema.FieldType.INT32)
                        .addNullableField("site_id0", Schema.FieldType.INT32)
                        .addNullableField("price0", Schema.FieldType.INT32)
                        .build())
                .addRows(
                    1, 2, 3, null, null, null, 2, 3, 3, null, null, null, 3, 4, 5, null, null, null)
                .getRows());
    pipeline.run();
  }

  @Test
  public void testInnerJoinWithEmptyTuplesOnRightSide() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " INNER JOIN (SELECT * FROM ORDER_DETAILS2 WHERE FALSE) o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("site_id", Schema.FieldType.INT32)
                        .addField("price", Schema.FieldType.INT32)
                        .addNullableField("order_id0", Schema.FieldType.INT32)
                        .addNullableField("site_id0", Schema.FieldType.INT32)
                        .addNullableField("price0", Schema.FieldType.INT32)
                        .build())
                .getRows());
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " RIGHT OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addNullableField("order_id", Schema.FieldType.INT32)
                        .addNullableField("site_id", Schema.FieldType.INT32)
                        .addNullableField("price", Schema.FieldType.INT32)
                        .addField("order_id0", Schema.FieldType.INT32)
                        .addField("site_id0", Schema.FieldType.INT32)
                        .addField("price0", Schema.FieldType.INT32)
                        .build())
                .addRows(2, 3, 3, 1, 2, 3, null, null, null, 2, 3, 3, null, null, null, 3, 4, 5)
                .getRows());
    pipeline.run();
  }

  @Test
  public void testFullOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " FULL OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addNullableField("order_id", Schema.FieldType.INT32)
                        .addNullableField("site_id", Schema.FieldType.INT32)
                        .addNullableField("price", Schema.FieldType.INT32)
                        .addNullableField("order_id0", Schema.FieldType.INT32)
                        .addNullableField("site_id0", Schema.FieldType.INT32)
                        .addNullableField("price0", Schema.FieldType.INT32)
                        .build())
                .addRows(
                    2, 3, 3, 1, 2, 3, 1, 2, 3, null, null, null, 3, 4, 5, null, null, null, null,
                    null, null, 2, 3, 3, null, null, null, 3, 4, 5)
                .getRows());
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_nonEqualJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id>o2.site_id";

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testException_join_condition1() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id = o2.site_id OR o1.price = o2.site_id";

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("Operator OR"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testException_join_condition2() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id = o2.site_id AND o1.price > o2.site_id";

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("Non equi-join"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testException_join_condition3() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id + o2.site_id = 2";

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("column reference"));
    thrown.expectMessage(StringContains.containsString("struct field access"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testException_join_condition4() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id + o2.site_id = 2 AND o1.price > o2.site_id";

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("column reference"));
    thrown.expectMessage(StringContains.containsString("struct field access"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_crossJoin() throws Exception {
    String sql = "SELECT *  " + "FROM ORDER_DETAILS1 o1, ORDER_DETAILS2 o2";

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }
}
