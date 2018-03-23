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

import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamSetOperatorRelBase}.
 */
public class BeamSetOperatorRelBaseTest extends BaseRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  public static final Date THE_DATE = new Date(100000);

  @BeforeClass
  public static void prepare() {
    sqlEnv.registerTable("ORDER_DETAILS",
        MockedBoundedTable.of(
            FieldType.INT64, "order_id",
            FieldType.INT32, "site_id",
            FieldType.DOUBLE, "price",
            FieldType.DATETIME, "order_time"
        ).addRows(
            1L, 1, 1.0, THE_DATE,
            2L, 2, 2.0, THE_DATE
        )
    );
  }

  @Test
  public void testSameWindow() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) ";

    PCollection<Row> rows = compilePipeline(sql, pipeline, sqlEnv);
    // compare valueInString to ignore the windowStart & windowEnd
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                FieldType.INT64, "order_id",
                FieldType.INT32, "site_id",
                FieldType.INT64, "cnt"
            ).addRows(
                1L, 1, 1L,
                2L, 2, 1L
            ).getStringRows());
    pipeline.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDifferentWindows() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '2' HOUR) ";

    // use a real pipeline rather than the TestPipeline because we are
    // testing exceptions, the pipeline will not actually run.
    Pipeline pipeline1 = Pipeline.create(PipelineOptionsFactory.create());
    compilePipeline(sql, pipeline1, sqlEnv);
    pipeline.run();
  }
}
