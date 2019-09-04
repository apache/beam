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
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamSqlOutputToConsoleFn;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestUnboundedTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Unbounded + Unbounded Test for {@code BeamSideInputJoinRel}. */
public class BeamSideInputJoinRelUnboundedVsBoundedTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  public static final DateTime FIRST_DATE = new DateTime(1);
  public static final DateTime SECOND_DATE = new DateTime(1 + 3600 * 1000);
  public static final DateTime THIRD_DATE = new DateTime(1 + 3600 * 1000 + 3600 * 1000 + 1);
  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDER_DETAILS",
        TestUnboundedTable.of(
                Schema.FieldType.INT32, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.INT32, "price",
                Schema.FieldType.DATETIME, "order_time")
            .timestampColumnIndex(3)
            .addRows(Duration.ZERO, 1, 1, 1, FIRST_DATE, 1, 2, 2, FIRST_DATE)
            .addRows(
                WINDOW_SIZE.plus(Duration.standardSeconds(1)),
                2,
                2,
                3,
                SECOND_DATE,
                2,
                3,
                3,
                SECOND_DATE,
                // this late data is omitted
                1,
                2,
                3,
                FIRST_DATE)
            .addRows(
                WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardSeconds(1)),
                3,
                3,
                3,
                THIRD_DATE,
                // this late data is omitted
                2,
                2,
                3,
                SECOND_DATE));

    registerTable(
        "ORDER_DETAILS1",
        TestBoundedTable.of(
                Schema.FieldType.INT32, "order_id",
                Schema.FieldType.STRING, "buyer")
            .addRows(
                1, "james",
                2, "bond"));

    registerTable(
        "SITE_LKP",
        new SiteLookupTable(
            TestTableUtils.buildBeamSqlSchema(
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.STRING, "site_name")));
  }

  /** Test table for JOIN-AS-LOOKUP. */
  public static class SiteLookupTable extends BaseBeamTable implements BeamSqlSeekableTable {

    public SiteLookupTable(Schema schema) {
      super(schema);
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      throw new UnsupportedOperationException();
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Row> seekRow(Row lookupSubRow) {
      return Arrays.asList(Row.withSchema(getSchema()).addValues(1, "SITE1").build());
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }
  }

  @Test
  public void testInnerJoin_unboundedTableOnTheLeftSide() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " JOIN "
            + " ORDER_DETAILS1 o2 "
            + " on "
            + " o1.order_id=o2.order_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.INT32, "sum_site_id",
                    Schema.FieldType.STRING, "buyer")
                .addRows(1, 3, "james", 2, 5, "bond")
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testInnerJoin_boundedTableOnTheLeftSide() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + " ORDER_DETAILS1 o2 "
            + " JOIN "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " on "
            + " o1.order_id=o2.order_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.INT32, "sum_site_id",
                    Schema.FieldType.STRING, "buyer")
                .addRows(1, 3, "james", 2, 5, "bond")
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testNodeStatsEstimation() {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " JOIN "
            + " ORDER_DETAILS1 o2 "
            + " on "
            + " o1.order_id=o2.order_id";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamSideInputJoinRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());
    NodeStats leftEstimate =
        BeamSqlRelUtils.getNodeStats(
            ((BeamSideInputJoinRel) root).getLeft(), root.getCluster().getMetadataQuery());
    NodeStats rightEstimate =
        BeamSqlRelUtils.getNodeStats(
            ((BeamSideInputJoinRel) root).getRight(), root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRowCount(), 0.01);

    Assert.assertNotEquals(0d, estimate.getRate(), 0.001);
    Assert.assertTrue(
        estimate.getRate()
            < leftEstimate.getRowCount() * rightEstimate.getWindow()
                + rightEstimate.getRowCount() * leftEstimate.getWindow());

    Assert.assertNotEquals(0d, estimate.getWindow(), 0.001);
    Assert.assertTrue(estimate.getWindow() < leftEstimate.getWindow() * rightEstimate.getWindow());
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " LEFT OUTER JOIN "
            + " ORDER_DETAILS1 o2 "
            + " on "
            + " o1.order_id=o2.order_id";

    PCollection<Row> rows = compilePipeline(sql, pipeline);

    rows.apply(ParDo.of(new BeamSqlOutputToConsoleFn("helloworld")));

    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("sum_site_id", Schema.FieldType.INT32)
                        .addNullableField("buyer", Schema.FieldType.STRING)
                        .build())
                .addRows(1, 3, "james", 2, 5, "bond", 3, 3, null)
                .getStringRows());
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLeftOuterJoinError() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + " ORDER_DETAILS1 o2 "
            + " LEFT OUTER JOIN "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " on "
            + " o1.order_id=o2.order_id";
    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + " ORDER_DETAILS1 o2 "
            + " RIGHT OUTER JOIN "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " on "
            + " o1.order_id=o2.order_id";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.builder()
                        .addField("order_id", Schema.FieldType.INT32)
                        .addField("sum_site_id", Schema.FieldType.INT32)
                        .addNullableField("buyer", Schema.FieldType.STRING)
                        .build())
                .addRows(1, 3, "james", 2, 5, "bond", 3, 3, null)
                .getStringRows());
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRightOuterJoinError() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " RIGHT OUTER JOIN "
            + " ORDER_DETAILS1 o2 "
            + " on "
            + " o1.order_id=o2.order_id";

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFullOuterJoinError() throws Exception {
    String sql =
        "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
            + " ORDER_DETAILS1 o2 "
            + " FULL OUTER JOIN "
            + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " on "
            + " o1.order_id=o2.order_id";
    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  public void testUnboundedVsLookupTableJoin() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " ORDER_DETAILS o1 "
            + " JOIN SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id "
            + " WHERE o1.site_id=1";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.STRING, "site_name")
                .addRows(1, "SITE1")
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testLookupTableVsUnboundedJoin() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " SITE_LKP o2 "
            + " JOIN ORDER_DETAILS o1 "
            + " on "
            + " o1.site_id=o2.site_id "
            + " WHERE o1.site_id=1";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.STRING, "site_name")
                .addRows(1, "SITE1")
                .getStringRows());
    pipeline.run();
  }
}
