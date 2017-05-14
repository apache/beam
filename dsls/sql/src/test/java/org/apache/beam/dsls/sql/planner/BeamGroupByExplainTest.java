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

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlUdfExpressionTest;
import org.junit.Test;

/**
 * Test group-by methods.
 *
 */
public class BeamGroupByExplainTest extends BasePlanner {

  /**
   * GROUP-BY without window operation, and grouped fields.
   */
  @Test
  public void testSimpleGroupExplain() throws Exception {
    String sql = "SELECT COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 ";
    String plan = runner.explainQuery(sql);
  }

  /**
   * GROUP-BY without window operation, and grouped fields.
   */
  @Test
  public void testSimpleGroup2Explain() throws Exception {
    String sql = "SELECT site_id" + ", COUNT(*) " + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY site_id";
    String plan = runner.explainQuery(sql);
  }

  /**
   * GROUP-BY with TUMBLE window.
   */
  @Test
  public void testTumbleExplain() throws Exception {
    String sql = "SELECT order_id, site_id" + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR)";
    String plan = runner.explainQuery(sql);
  }

  /**
   * GROUP-BY with TUMBLE window.
   */
  @Test
  public void testTumbleWithDelayExplain() throws Exception {
    String sql = "SELECT order_id, site_id, "
        + "TUMBLE_START(order_time, INTERVAL '1' HOUR, TIME '00:00:01')"
        + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS " + "WHERE SITE_ID = 0 "
        + "GROUP BY order_id, site_id" + ", TUMBLE(order_time, INTERVAL '1' HOUR, TIME '00:00:01')";
    String plan = runner.explainQuery(sql);
  }

  /**
   * GROUP-BY with HOP window.
   */
  @Test
  public void testHopExplain() throws Exception {
    String sql = "SELECT order_id, site_id" + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY order_id, site_id"
        + ", HOP(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)";
    String plan = runner.explainQuery(sql);
  }

  /**
   * GROUP-BY with SESSION window.
   */
  @Test
  public void testSessionExplain() throws Exception {
    String sql = "SELECT order_id, site_id" + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 " + "GROUP BY order_id, site_id"
        + ", SESSION(order_time, INTERVAL '5' MINUTE)";
    String plan = runner.explainQuery(sql);
  }

  /**
   * Query with UDF.
   */
  @Test
  public void testUdf() throws Exception {
    runner.addUDFFunction("negative", BeamSqlUdfExpressionTest.UdfFn.class, "negative");
    String sql = "select site_id, negative(site_id) as nsite_id from ORDER_DETAILS";

    String plan = runner.explainQuery(sql);
  }
}
