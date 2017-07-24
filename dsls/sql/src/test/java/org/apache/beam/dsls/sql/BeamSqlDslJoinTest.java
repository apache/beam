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

package org.apache.beam.dsls.sql;

import static org.apache.beam.dsls.sql.rel.BeamJoinRelBoundedVsBoundedTest.ORDER_DETAILS1;
import static org.apache.beam.dsls.sql.rel.BeamJoinRelBoundedVsBoundedTest.ORDER_DETAILS2;

import java.sql.Types;
import java.util.Arrays;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.sd.BeamRowCoder;
import org.apache.beam.sdk.sd.BeamRowType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for joins in queries.
 */
public class BeamSqlDslJoinTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  private static final BeamRowType SOURCE_RECORD_TYPE =
      BeamRowType.create(
          Arrays.asList(
              "order_id", "site_id", "price"
          ),
          Arrays.asList(
              Types.INTEGER, Types.INTEGER, Types.INTEGER
          )
      );

  private static final BeamRowCoder SOURCE_CODER =
      new BeamRowCoder(SOURCE_RECORD_TYPE);

  private static final BeamRowType RESULT_RECORD_TYPE =
      BeamRowType.create(
          Arrays.asList(
          "order_id", "site_id", "price", "order_id0", "site_id0", "price0"
          ),
          Arrays.asList(
              Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER
              , Types.INTEGER, Types.INTEGER
          )
      );

  private static final BeamRowCoder RESULT_CODER =
      new BeamRowCoder(RESULT_RECORD_TYPE);

  @Test
  public void testInnerJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_RECORD_TYPE
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
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_RECORD_TYPE
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
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_RECORD_TYPE
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
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PAssert.that(queryFromOrderTables(sql)).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            RESULT_RECORD_TYPE
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
            + " o1.order_id>o2.site_id"
        ;

    pipeline.enableAbandonedNodeEnforcement(false);
    queryFromOrderTables(sql);
    pipeline.run();
  }

  @Test(expected = IllegalStateException.class)
  public void testException_crossJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1, ORDER_DETAILS2 o2";

    pipeline.enableAbandonedNodeEnforcement(false);
    queryFromOrderTables(sql);
    pipeline.run();
  }

  private PCollection<BeamRow> queryFromOrderTables(String sql) {
    return PCollectionTuple
        .of(
            new TupleTag<BeamRow>("ORDER_DETAILS1"),
            ORDER_DETAILS1.buildIOReader(pipeline).setCoder(SOURCE_CODER)
        )
        .and(new TupleTag<BeamRow>("ORDER_DETAILS2"),
            ORDER_DETAILS2.buildIOReader(pipeline).setCoder(SOURCE_CODER)
        ).apply("join", BeamSql.query(sql)).setCoder(RESULT_CODER);
  }
}
