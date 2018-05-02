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
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Bounded + Bounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelBoundedVsBoundedTest extends BaseRelTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  public static final MockedBoundedTable ORDER_DETAILS1 =
      MockedBoundedTable.of(
          TypeName.INT32, "order_id",
          TypeName.INT32, "site_id",
          TypeName.INT32, "price"
      ).addRows(
          1, 2, 3,
          2, 3, 3,
          3, 4, 5
      );

  public static final MockedBoundedTable ORDER_DETAILS2 =
      MockedBoundedTable.of(
          TypeName.INT32, "order_id",
          TypeName.INT32, "site_id",
          TypeName.INT32, "price"
      ).addRows(
          1, 2, 3,
          2, 3, 3,
          3, 4, 5
      );

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
        + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            TypeName.INT32, "order_id",
            TypeName.INT32, "site_id",
            TypeName.INT32, "price",
            TypeName.INT32, "order_id0",
            TypeName.INT32, "site_id0",
            TypeName.INT32, "price0"
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

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            TypeName.INT32, "order_id",
            TypeName.INT32, "site_id",
            TypeName.INT32, "price",
            TypeName.INT32, "order_id0",
            TypeName.INT32, "site_id0",
            TypeName.INT32, "price0"
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

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            TypeName.INT32, "order_id",
            TypeName.INT32, "site_id",
            TypeName.INT32, "price",
            TypeName.INT32, "order_id0",
            TypeName.INT32, "site_id0",
            TypeName.INT32, "price0"
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

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            TypeName.INT32, "order_id",
            TypeName.INT32, "site_id",
            TypeName.INT32, "price",
            TypeName.INT32, "order_id0",
            TypeName.INT32, "site_id0",
            TypeName.INT32, "price0"
        ).addRows(
          2, 3, 3, 1, 2, 3,
          1, 2, 3, null, null, null,
          3, 4, 5, null, null, null,
          null, null, null, 2, 3, 3,
          null, null, null, 3, 4, 5
        ).getRows());
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_nonEqualJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id>o2.site_id"
        ;

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_crossJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1, ORDER_DETAILS2 o2";

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline);
    pipeline.run();
  }
}
