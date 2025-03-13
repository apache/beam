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

import static org.apache.beam.sdk.extensions.sql.impl.rel.BeamCoGBKJoinRelBoundedVsBoundedTest.ORDER_DETAILS1;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BeamSideInputLookupJoinRelTest extends BaseRelTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  private static final boolean nullable = true;

  /** Test table for JOIN-AS-LOOKUP. */
  public static class SiteLookupTable extends SchemaBaseBeamTable implements BeamSqlSeekableTable {
    private Schema joinSubsetType;

    public SiteLookupTable(Schema schema) {
      super(schema);
    }

    @Override
    public void setUp(Schema joinSubsetType) {
      this.joinSubsetType = joinSubsetType;
      Assert.assertNotNull(joinSubsetType);
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
      Assert.assertEquals(joinSubsetType, lookupSubRow.getSchema());
      if (lookupSubRow.getInt32("site_id") == 2) {
        return Arrays.asList(Row.withSchema(getSchema()).addValues(2, "SITE1").build());
      }
      return Arrays.asList(Row.nullRow(getSchema()));
    }
  }

  @BeforeClass
  public static void prepare() {
    BeamSideInputJoinRelTest.registerUnboundedTable();
    registerTable("ORDER_DETAILS1", ORDER_DETAILS1);
    registerTable(
        "SITE_LKP",
        new SiteLookupTable(
            TestTableUtils.buildBeamSqlNullableSchema(
                Schema.FieldType.INT32,
                "site_id",
                nullable,
                Schema.FieldType.STRING,
                "site_name",
                nullable)));
  }

  @Test
  public void testBoundedTableInnerJoinWithLookupTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " ORDER_DETAILS1 o1 "
            + " JOIN SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id "
            + " WHERE o1.site_id=2 ";
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
  public void testLookupTableInnerJoinWithBoundedTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " SITE_LKP o2 "
            + " JOIN ORDER_DETAILS1 o1 "
            + " on "
            + " o1.site_id=o2.site_id "
            + " WHERE o1.site_id=2 ";
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
  public void testUnboundedTableInnerJoinWithLookupTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + "(select order_id, site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " JOIN "
            + " SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id"
            + " WHERE o1.site_id=2 ";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.STRING, "site_name")
                .addRows(1, "SITE1")
                .addRows(2, "SITE1")
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testLookupTableInnerJoinWithUnboundedTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " SITE_LKP o2 "
            + " JOIN "
            + "(select order_id, site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " on "
            + " o1.site_id=o2.site_id"
            + " WHERE o1.site_id=2 ";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "order_id",
                    Schema.FieldType.STRING, "site_name")
                .addRows(1, "SITE1")
                .addRows(2, "SITE1")
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testLookupTableRightOuterJoinWithBoundedTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " SITE_LKP o2 "
            + " RIGHT OUTER JOIN "
            + " ORDER_DETAILS1 o1 "
            + " on "
            + " o1.site_id=o2.site_id ";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.ofNullable(
                    Schema.FieldType.INT32,
                    "order_id",
                    nullable,
                    Schema.FieldType.STRING,
                    "site_name",
                    nullable)
                .addRows(1, "SITE1")
                .addRows(2, null)
                .addRows(3, null)
                .getStringRows());
    pipeline.run();
  }

  @Test
  public void testUnboundedTableLeftOuterJoinWithLookupTable() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + "(select order_id, site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " LEFT OUTER JOIN "
            + " SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id";
    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.ofNullable(
                    Schema.FieldType.INT32,
                    "order_id",
                    nullable,
                    Schema.FieldType.STRING,
                    "site_name",
                    nullable)
                .addRows(1, "SITE1")
                .addRows(2, "SITE1")
                .addRows(1, null)
                .addRows(2, null)
                .addRows(3, null)
                .getStringRows());
    pipeline.run();
  }

  @Test
  // Do not add a filter like "WHERE o1.order_id=2". By adding that filter, FilterJoinRule may
  // convert "LEFT OUTER JOIN" to "INNER JOIN".
  public void testLookupTableLeftOuterJoinWithBoundedTableError() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + " SITE_LKP o2 "
            + " LEFT OUTER JOIN "
            + " ORDER_DETAILS1 o1 "
            + " on "
            + " o1.site_id=o2.site_id ";
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("OUTER JOIN must be a non Seekable table"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  // Do not add a filter like "WHERE o1.order_id=2". By adding that filter, FilterJoinRule may
  // convert "FULL OUTER JOIN" to "LEFT OUTER JOIN", which, in tis case is a valid scenario.
  public void testUnboundedTableFullOuterJoinWithLookupTableError() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + "(select order_id, site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " FULL OUTER JOIN "
            + " SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id";
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("not supported"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }

  @Test
  // Do not add a filter like "WHERE o1.order_id=2". By adding that filter, FilterJoinRule may
  // convert "RIGHT OUTER JOIN" to "INNER JOIN".
  public void testUnboundedTableRightOuterJoinWithLookupTableError() throws Exception {
    String sql =
        "SELECT o1.order_id, o2.site_name FROM "
            + "(select order_id, site_id FROM ORDER_DETAILS "
            + "          GROUP BY order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
            + " RIGHT OUTER JOIN "
            + " SITE_LKP o2 "
            + " on "
            + " o1.site_id=o2.site_id";
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(StringContains.containsString("OUTER JOIN must be a non Seekable table"));
    compilePipeline(sql, pipeline);
    pipeline.run();
  }
}
