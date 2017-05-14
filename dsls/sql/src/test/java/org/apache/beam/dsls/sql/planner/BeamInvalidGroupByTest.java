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

import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.tools.ValidationException;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test group-by methods.
 *
 */
public class BeamInvalidGroupByTest extends BasePlanner {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test(expected = ValidationException.class)
  public void testTumble2Explain() throws Exception {
    String sql = "SELECT order_id, site_id" + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY order_id" + ", TUMBLE(order_time, INTERVAL '1' HOUR)";
    PCollection<BeamSQLRow> outputStream = runner.compileBeamPipeline(sql, pipeline);
  }

  @Test(expected = ValidationException.class)
  public void testTumble3Explain() throws Exception {
    String sql = "SELECT order_id, site_id, TUMBLE(order_time, INTERVAL '1' HOUR)"
        + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS " + "WHERE SITE_ID = 0 "
        + "GROUP BY order_id, site_id" + ", TUMBLE(order_time, INTERVAL '1' HOUR)";
    PCollection<BeamSQLRow> outputStream = runner.compileBeamPipeline(sql, pipeline);
  }

}
