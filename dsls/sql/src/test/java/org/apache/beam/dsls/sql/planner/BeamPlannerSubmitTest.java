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

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests to execute a query.
 *
 */
public class BeamPlannerSubmitTest extends BasePlanner {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void prepare() {
    MockedBeamSQLTable.CONTENT.clear();
  }

  @Test
  public void insertSelectFilter() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price) SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS " + "WHERE SITE_ID = 0 and price > 20";

    PCollection<BeamSQLRow> outputStream = BeamSqlCli.compilePipeline(sql, pipeline);

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(MockedBeamSQLTable.CONTENT.size() == 1);
    Assert.assertTrue(MockedBeamSQLTable.CONTENT.peek().valueInString()
        .contains("order_id=12345,site_id=0,price=20.5,order_time="));
  }
}
