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
package org.apache.beam.dsls.sql.planner;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to execute a query.
 *
 */
public class BeamPlannerAggregationSubmitTest {
  public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  public static BeamSqlRunner runner = new BeamSqlRunner();

  @BeforeClass
  public static void prepare() throws ParseException {
    runner.addTable("ORDER_DETAILS", getOrderTable());
    runner.addTable("ORDER_SUMMARY", getSummaryTable());
  }

  private static BaseBeamTable getOrderTable() throws ParseException {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("order_time", SqlTypeName.TIMESTAMP).build();
      }
    };

    BeamSQLRecordType dataType = BeamSQLRecordType.from(
        protoRowType.apply(BeamQueryPlanner.TYPE_FACTORY));
    BeamSQLRow row1 = new BeamSQLRow(dataType);
    row1.addField(0, 12345L);
    row1.addField(1, 1);
    row1.addField(2, format.parse("2017-01-01 01:02:03"));

    BeamSQLRow row2 = new BeamSQLRow(dataType);
    row2.addField(0, 12345L);
    row2.addField(1, 0);
    row2.addField(2, format.parse("2017-01-01 01:03:04"));

    BeamSQLRow row3 = new BeamSQLRow(dataType);
    row3.addField(0, 12345L);
    row3.addField(1, 0);
    row3.addField(2, format.parse("2017-01-01 02:03:04"));

    BeamSQLRow row4 = new BeamSQLRow(dataType);
    row4.addField(0, 2132L);
    row4.addField(1, 0);
    row4.addField(2, format.parse("2017-01-01 03:04:05"));

    return new MockedBeamSQLTable(protoRowType).withInputRecords(
        Arrays.asList(row1
            , row2, row3, row4
            ));

  }

  private static BaseBeamTable getSummaryTable() {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("site_id", SqlTypeName.INTEGER)
            .add("agg_hour", SqlTypeName.TIMESTAMP)
            .add("size", SqlTypeName.BIGINT).build();
      }
    };
    return new MockedBeamSQLTable(protoRowType);
  }


  @Test
  public void selectWithWindowAggregation() throws Exception{
    String sql = "INSERT INTO ORDER_SUMMARY(SITE_ID, agg_hour, SIZE) "
        + "SELECT site_id, TUMBLE_START(order_time, INTERVAL '1' HOUR, TIME '00:00:01')"
        + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 1 " + "GROUP BY site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR, TIME '00:00:01')";

    Pipeline pipeline = runner.getPlanner().compileBeamPipeline(sql);

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(MockedBeamSQLTable.CONTENT.size() == 1);
    BeamSQLRow result = MockedBeamSQLTable.CONTENT.get(0);
    Assert.assertEquals(1, result.getInteger(0));
    Assert.assertEquals(format.parse("2017-01-01 01:00:00"), result.getDate(1));
    Assert.assertEquals(1L, result.getLong(2));
  }

  @Test
  public void selectWithoutWindowAggregation() throws Exception{
    String sql = "INSERT INTO ORDER_SUMMARY(SITE_ID, SIZE) "
        + "SELECT site_id, COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY site_id";

    Pipeline pipeline = runner.getPlanner().compileBeamPipeline(sql);

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(MockedBeamSQLTable.CONTENT.size() == 1);
    Assert.assertEquals("site_id=0,agg_hour=null,size=3",
        MockedBeamSQLTable.CONTENT.get(0).valueInString());
  }
}
