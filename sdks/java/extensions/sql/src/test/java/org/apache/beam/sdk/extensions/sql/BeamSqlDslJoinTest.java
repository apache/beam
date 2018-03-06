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

import static org.apache.beam.sdk.extensions.sql.TestUtils.tuple;
import static org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRelBoundedVsBoundedTest.ORDER_DETAILS1;
import static org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRelBoundedVsBoundedTest.ORDER_DETAILS2;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.stringContainsInOrder;

import java.util.Arrays;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for joins in queries.
 */
public class BeamSqlDslJoinTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final RowType SOURCE_ROW_TYPE =
      RowSqlType.builder()
          .withIntegerField("order_id")
          .withIntegerField("site_id")
          .withIntegerField("price")
          .build();

  private static final RowCoder SOURCE_CODER = SOURCE_ROW_TYPE.getRowCoder();

  private static final RowType RESULT_ROW_TYPE =
      RowSqlType.builder()
          .withIntegerField("order_id")
          .withIntegerField("site_id")
          .withIntegerField("price")
          .withIntegerField("order_id0")
          .withIntegerField("site_id0")
          .withIntegerField("price0")
          .build();

  private static final RowCoder RESULT_CODER = RESULT_ROW_TYPE.getRowCoder();

  @Test
  public void testInnerJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_ROW_TYPE
        ).addRows(
            2, 3, 3, 1, 2, 3
        ).getRows());
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " LEFT OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_ROW_TYPE
        ).addRows(
            1, 2, 3, null, null, null,
            2, 3, 3, 1, 2, 3,
            3, 4, 5, null, null, null
        ).getRows());
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

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_ROW_TYPE
        ).addRows(
            2, 3, 3, 1, 2, 3,
            null, null, null, 2, 3, 3,
            null, null, null, 3, 4, 5
        ).getRows());
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

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_ROW_TYPE
        ).addRows(
            2, 3, 3, 1, 2, 3,
            1, 2, 3, null, null, null,
            3, 4, 5, null, null, null,
            null, null, null, 2, 3, 3,
            null, null, null, 3, 4, 5
        ).getRows());
    pipeline.run();
  }

  @Test(expected = IllegalStateException.class)
  public void testException_nonEqualJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id>o2.site_id";

    pipeline.enableAbandonedNodeEnforcement(false);
    queryFromOrderTables(sql);
    pipeline.run();
  }

  @Test
  public void testException_crossJoin() throws Exception {
    thrown.expect(IllegalStateException.class);

    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1, ORDER_DETAILS2 o2";

    pipeline.enableAbandonedNodeEnforcement(false);
    queryFromOrderTables(sql);
    pipeline.run();
  }

  @Test
  public void testJoinsUnboundedWithinWindowsWithDefaultTrigger() throws Exception {

    String sql =
        "SELECT o1.order_id, o1.price, o1.site_id, o2.order_id, o2.price, o2.site_id  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> orders = ordersUnbounded()
        .apply("window", Window.into(FixedWindows.of(Duration.standardSeconds(50))));
    PCollectionTuple inputs = tuple("ORDER_DETAILS1", orders, "ORDER_DETAILS2", orders);

    PAssert
        .that(
            inputs.apply("sql", BeamSql.query(sql)))
        .containsInAnyOrder(
        TestUtils.RowsBuilder
            .of(
            RESULT_ROW_TYPE
        ).addRows(
            1, 2, 2, 2, 2, 1,
            1, 4, 3, 3, 3, 1
        ).getRows());

    pipeline.run();
  }

  @Test
  public void testRejectsUnboundedWithinWindowsWithEndOfWindowTrigger() throws Exception {

    String sql =
        "SELECT o1.order_id, o1.price, o1.site_id, o2.order_id, o2.price, o2.site_id  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> orders = ordersUnbounded()
        .apply("window",
               Window
                   .<Row>into(FixedWindows.of(Duration.standardSeconds(50)))
                   .triggering(AfterWatermark.pastEndOfWindow())
                   .withAllowedLateness(Duration.ZERO)
                   .accumulatingFiredPanes());
    PCollectionTuple inputs = tuple("ORDER_DETAILS1", orders, "ORDER_DETAILS2", orders);

    thrown.expectCause(expectedSingleFireTrigger());

    inputs.apply("sql", BeamSql.query(sql));

    pipeline.run();
  }

  @Test
  public void testRejectsGlobalWindowsWithDefaultTriggerInUnboundedInput() throws Exception {

    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> orders = ordersUnbounded();
    PCollectionTuple inputs = tuple("ORDER_DETAILS1", orders, "ORDER_DETAILS2", orders);

    thrown.expectCause(expectedSingleFireTrigger());

    inputs.apply("sql", BeamSql.query(sql));

    pipeline.run();
  }

  @Test
  public void testRejectsGlobalWindowsWithEndOfWindowTrigger() throws Exception {

    String sql =
        "SELECT o1.order_id, o1.price, o1.site_id, o2.order_id, o2.price, o2.site_id  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> orders = ordersUnbounded()
        .apply("window",
               Window
                   .<Row>into(new GlobalWindows())
                   .triggering(AfterWatermark.pastEndOfWindow())
                   .withAllowedLateness(Duration.ZERO)
                   .accumulatingFiredPanes());
    PCollectionTuple inputs = tuple("ORDER_DETAILS1", orders, "ORDER_DETAILS2", orders);

    thrown.expectCause(expectedSingleFireTrigger());

    inputs.apply("sql", BeamSql.query(sql));

    pipeline.run();
  }

  @Test
  public void testRejectsNonGlobalWindowsWithRepeatingTrigger() throws Exception {

    String sql =
        "SELECT o1.order_id, o1.price, o1.site_id, o2.order_id, o2.price, o2.site_id  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id";

    PCollection<Row> orders = ordersUnbounded()
        .apply(
            "window",
            Window
                .<Row>into(FixedWindows.of(Duration.standardSeconds(203)))
                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                .withAllowedLateness(Duration.standardMinutes(2))
                .accumulatingFiredPanes());
    PCollectionTuple inputs = tuple("ORDER_DETAILS1", orders, "ORDER_DETAILS2", orders);

    thrown.expectCause(expectedSingleFireTrigger());

    inputs.apply("sql", BeamSql.query(sql));

    pipeline.run();
  }

  private PCollection<Row> ordersUnbounded() {
    DateTime ts = new DateTime(2017, 1, 1, 1, 0, 0);

    return
        TestUtils
            .rowsBuilderOf(
                RowSqlType
                    .builder()
                    .withIntegerField("order_id")
                    .withIntegerField("price")
                    .withIntegerField("site_id")
                    .withTimestampField("timestamp")
                    .build())
            .addRows(
                1, 2, 2, ts.plusSeconds(0).toDate(),
                2, 2, 1, ts.plusSeconds(40).toDate(),
                1, 4, 3, ts.plusSeconds(60).toDate(),
                3, 2, 1, ts.plusSeconds(65).toDate(),
                3, 3, 1, ts.plusSeconds(70).toDate())
            .getPCollectionBuilder()
            .withTimestampField("timestamp")
            .inPipeline(pipeline)
            .buildUnbounded();
  }

  private PCollection<Row> queryFromOrderTables(String sql) {
    return tuple(
        "ORDER_DETAILS1", ORDER_DETAILS1.buildIOReader(pipeline).setCoder(SOURCE_CODER),
        "ORDER_DETAILS2", ORDER_DETAILS2.buildIOReader(pipeline).setCoder(SOURCE_CODER))
        .apply("join", BeamSql.query(sql))
        .setCoder(RESULT_CODER);
  }

  private Matcher<UnsupportedOperationException> expectedSingleFireTrigger() {
    return allOf(
        isA(UnsupportedOperationException.class),
        hasProperty("message",
                    stringContainsInOrder(
                        Arrays.asList("once per window", "default trigger"))));
  }
}
