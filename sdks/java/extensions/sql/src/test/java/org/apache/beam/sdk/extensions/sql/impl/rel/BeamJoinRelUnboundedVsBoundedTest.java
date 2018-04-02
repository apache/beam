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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamSqlOutputToConsoleFn;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.extensions.sql.mock.MockedUnboundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unbounded + Unbounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelUnboundedVsBoundedTest extends BaseRelTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static final BeamSqlEnv BEAM_SQL_ENV = new BeamSqlEnv();
  public static final DateTime FIRST_DATE = new DateTime(1);
  public static final DateTime SECOND_DATE = new DateTime(1 + 3600 * 1000);
  public static final DateTime THIRD_DATE = new DateTime(1 + 3600 * 1000 + 3600 * 1000 + 1);
  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    BEAM_SQL_ENV.registerTable("ORDER_DETAILS", MockedUnboundedTable
        .of(
            TypeName.INT32, "order_id",
            TypeName.INT32, "site_id",
            TypeName.INT32, "price",
            TypeName.DATETIME, "order_time"
        )
        .timestampColumnIndex(3)
        .addRows(
            Duration.ZERO,
            1, 1, 1, FIRST_DATE,
            1, 2, 2, FIRST_DATE
        )
        .addRows(
            WINDOW_SIZE.plus(Duration.standardSeconds(1)),
            2, 2, 3, SECOND_DATE,
            2, 3, 3, SECOND_DATE,
            // this late data is omitted
            1, 2, 3, FIRST_DATE
        )
        .addRows(
            WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardSeconds(1)),
            3, 3, 3, THIRD_DATE,
            // this late data is omitted
            2, 2, 3, SECOND_DATE
        )
    );

    BEAM_SQL_ENV.registerTable("ORDER_DETAILS1", MockedBoundedTable
        .of(TypeName.INT32, "order_id",
            TypeName.STRING, "buyer"
        ).addRows(
            1, "james",
            2, "bond"
        ));

    BEAM_SQL_ENV.registerTable(
        "SITE_LKP",
        new SiteLookupTable(
            TestUtils.buildBeamSqlRowType(
                TypeName.INT32, "site_id",
                TypeName.STRING, "site_name")));
  }

  /**
   * Test table for JOIN-AS-LOOKUP.
   *
   */
  public static class SiteLookupTable extends BaseBeamTable implements BeamSqlSeekableTable{

    public SiteLookupTable(Schema schema) {
      super(schema);
    }

    @Override
    public BeamIOType getSourceType() {
      return BeamIOType.BOUNDED;
    }

    @Override
    public PCollection<Row> buildIOReader(Pipeline pipeline) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PTransform<? super PCollection<Row>, PDone> buildIOWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Row> seekRow(Row lookupSubRow) {
      return Arrays.asList(Row.withSchema(getSchema()).addValues(1, "SITE1").build());
    }
  }

  @Test
  public void testInnerJoin_unboundedTableOnTheLeftSide() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                TypeName.INT32, "order_id",
                TypeName.INT32, "sum_site_id",
                TypeName.STRING, "buyer"
            ).addRows(
                1, 3, "james",
                2, 5, "bond"
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test
  public void testInnerJoin_boundedTableOnTheLeftSide() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                TypeName.INT32, "order_id",
                TypeName.INT32, "sum_site_id",
                TypeName.STRING, "buyer"
            ).addRows(
                1, 3, "james",
                2, 5, "bond"
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " LEFT OUTER JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    rows.apply(ParDo.of(new BeamSqlOutputToConsoleFn("helloworld")));
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                TypeName.INT32, "order_id",
                TypeName.INT32, "sum_site_id",
                TypeName.STRING, "buyer"
            ).addRows(
                1, 3, "james",
                2, 5, "bond",
                3, 3, null
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLeftOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " LEFT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " RIGHT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                TypeName.INT32, "order_id",
                TypeName.INT32, "sum_site_id",
                TypeName.STRING, "buyer"
            ).addRows(
                1, 3, "james",
                2, 5, "bond",
                3, 3, null
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRightOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " RIGHT OUTER JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFullOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " FULL OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.run();
  }

  @Test
  public void testJoinAsLookup() throws Exception {
    String sql = "SELECT o1.order_id, o2.site_name FROM "
        + " ORDER_DETAILS o1 "
        + " JOIN SITE_LKP o2 "
        + " on "
        + " o1.site_id=o2.site_id "
        + " WHERE o1.site_id=1"
        ;
    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                TypeName.INT32, "order_id",
                TypeName.STRING, "site_name"
            ).addRows(
                1, "SITE1"
            ).getStringRows()
        );
    pipeline.run();
  }
}
