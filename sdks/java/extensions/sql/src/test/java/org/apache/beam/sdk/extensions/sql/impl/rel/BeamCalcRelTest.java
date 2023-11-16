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

import java.math.BigDecimal;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestUnboundedTable;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Tests related to {@code BeamCalcRel}. */
public class BeamCalcRelTest extends BaseRelTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final DateTime FIRST_DATE = new DateTime(1);
  private static final DateTime SECOND_DATE = new DateTime(1 + 3600 * 1000);

  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDER_DETAILS_BOUNDED",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DECIMAL, "price")
            .addRows(
                1L,
                1,
                new BigDecimal(1.0),
                1L,
                1,
                new BigDecimal(1.0),
                2L,
                2,
                new BigDecimal(2.0),
                4L,
                4,
                new BigDecimal(4.0),
                4L,
                4,
                new BigDecimal(4.0)));

    registerTable(
        "ORDER_DETAILS_UNBOUNDED",
        TestUnboundedTable.of(
                Schema.FieldType.INT32, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.INT32, "price",
                Schema.FieldType.DATETIME, "order_time")
            .timestampColumnIndex(3)
            .addRows(Duration.ZERO, 1, 1, 1, FIRST_DATE, 1, 2, 6, FIRST_DATE)
            .addRows(
                WINDOW_SIZE.plus(Duration.standardMinutes(1)),
                2,
                2,
                7,
                SECOND_DATE,
                2,
                3,
                8,
                SECOND_DATE,
                // this late record is omitted(First window)
                1,
                3,
                3,
                FIRST_DATE)
            .addRows(
                // this late record is omitted(Second window)
                WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardMinutes(1)),
                2,
                3,
                3,
                SECOND_DATE)
            .setStatistics(BeamTableStatistics.createUnboundedTableStatistics(2d)));
  }

  @Test
  public void testProjectionNodeStats() {
    String sql = "SELECT order_id FROM ORDER_DETAILS_BOUNDED";

    RelNode root = env.parseQuery(sql);

    Assert.assertTrue(root instanceof BeamCalcRel);

    NodeStats estimate =
        BeamSqlRelUtils.getNodeStats(
            root, ((BeamRelMetadataQuery) root.getCluster().getMetadataQuery()));

    Assert.assertEquals(5d, estimate.getRowCount(), 0.001);
    Assert.assertEquals(5d, estimate.getWindow(), 0.001);
    Assert.assertEquals(0., estimate.getRate(), 0.001);
  }

  @Test
  public void testFilterNodeStats() {
    String sql = "SELECT * FROM ORDER_DETAILS_BOUNDED where order_id=1";

    RelNode root = env.parseQuery(sql);

    Assert.assertTrue(root instanceof BeamCalcRel);

    NodeStats estimate =
        BeamSqlRelUtils.getNodeStats(
            root, ((BeamRelMetadataQuery) root.getCluster().getMetadataQuery()));

    Assert.assertTrue(5d > estimate.getRowCount());
    Assert.assertTrue(5d > estimate.getWindow());
    Assert.assertEquals(0., estimate.getRate(), 0.001);
  }

  @Test
  public void testNodeStatsConditionType() {
    String equalSql = "SELECT * FROM ORDER_DETAILS_BOUNDED where order_id=1";
    String geqSql = "SELECT * FROM ORDER_DETAILS_BOUNDED where order_id>=1";

    RelNode equalRoot = env.parseQuery(equalSql);
    RelNode geqRoot = env.parseQuery(geqSql);

    NodeStats equalEstimate =
        BeamSqlRelUtils.getNodeStats(
            equalRoot, ((BeamRelMetadataQuery) equalRoot.getCluster().getMetadataQuery()));
    NodeStats geqEstimate =
        BeamSqlRelUtils.getNodeStats(
            geqRoot, ((BeamRelMetadataQuery) geqRoot.getCluster().getMetadataQuery()));

    Assert.assertTrue(geqEstimate.getRowCount() > equalEstimate.getRowCount());
    Assert.assertTrue(geqEstimate.getWindow() > equalEstimate.getWindow());
  }

  @Test
  public void testNodeStatsNumberOfConditions() {
    String equalSql = "SELECT * FROM ORDER_DETAILS_BOUNDED where order_id=1";
    String doubleEqualSql = "SELECT * FROM ORDER_DETAILS_BOUNDED WHERE order_id=1 AND site_id=2 ";

    RelNode equalRoot = env.parseQuery(equalSql);
    RelNode doubleEqualRoot = env.parseQuery(doubleEqualSql);

    NodeStats equalEstimate =
        BeamSqlRelUtils.getNodeStats(
            equalRoot, ((BeamRelMetadataQuery) equalRoot.getCluster().getMetadataQuery()));
    NodeStats doubleEqualEstimate =
        BeamSqlRelUtils.getNodeStats(
            doubleEqualRoot,
            ((BeamRelMetadataQuery) doubleEqualRoot.getCluster().getMetadataQuery()));

    Assert.assertTrue(doubleEqualEstimate.getRowCount() < equalEstimate.getRowCount());
    Assert.assertTrue(doubleEqualEstimate.getWindow() < equalEstimate.getWindow());
  }

  private static class NodeGetter extends Pipeline.PipelineVisitor.Defaults {

    private final PValue target;
    private TransformHierarchy.Node producer;

    private NodeGetter(PValue target) {
      this.target = target;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (value == target) {
        assert this.producer == null;
        this.producer = producer;
      }
    }
  }

  @Test
  public void testSingleFieldAccess() throws IllegalAccessException {
    String sql = "SELECT order_id FROM ORDER_DETAILS_BOUNDED";

    PCollection<Row> rows = compilePipeline(sql, pipeline);

    final NodeGetter nodeGetter = new NodeGetter(rows);
    pipeline.traverseTopologically(nodeGetter);

    ParDo.MultiOutput<Row, Row> pardo =
        (ParDo.MultiOutput<Row, Row>) nodeGetter.producer.getTransform();
    PCollection<Row> input =
        (PCollection<Row>) Iterables.getOnlyElement(nodeGetter.producer.getInputs().values());

    DoFnSchemaInformation info = ParDo.getDoFnSchemaInformation(pardo.getFn(), input);

    FieldAccessDescriptor fieldAccess = info.getFieldAccessDescriptor();

    Assert.assertTrue(fieldAccess.referencesSingleField());
    Assert.assertEquals("order_id", Iterables.getOnlyElement(fieldAccess.fieldNamesAccessed()));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNoFieldAccess() throws IllegalAccessException {
    String sql = "SELECT 1 FROM ORDER_DETAILS_BOUNDED";

    PCollection<Row> rows = compilePipeline(sql, pipeline);

    final NodeGetter nodeGetter = new NodeGetter(rows);
    pipeline.traverseTopologically(nodeGetter);

    ParDo.MultiOutput<Row, Row> pardo =
        (ParDo.MultiOutput<Row, Row>) nodeGetter.producer.getTransform();
    PCollection<Row> input =
        (PCollection<Row>) Iterables.getOnlyElement(nodeGetter.producer.getInputs().values());

    DoFnSchemaInformation info = ParDo.getDoFnSchemaInformation(pardo.getFn(), input);

    FieldAccessDescriptor fieldAccess = info.getFieldAccessDescriptor();

    Assert.assertFalse(fieldAccess.getAllFields());
    Assert.assertTrue(fieldAccess.getFieldsAccessed().isEmpty());
    Assert.assertTrue(fieldAccess.getNestedFieldsAccessed().isEmpty());

    pipeline.run().waitUntilFinish();
  }
}
