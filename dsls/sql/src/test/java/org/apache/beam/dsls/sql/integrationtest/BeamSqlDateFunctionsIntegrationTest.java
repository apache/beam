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

package org.apache.beam.dsls.sql.integrationtest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Iterator;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Integration test for date functions.
 */
public class BeamSqlDateFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test public void testDateTimeFunctions() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("EXTRACT(YEAR FROM ts)", 1986L)
        .addExpr("YEAR(ts)", 1986L)
        .addExpr("QUARTER(ts)", 1L)
        .addExpr("MONTH(ts)", 2L)
        .addExpr("WEEK(ts)", 7L)
        .addExpr("DAYOFMONTH(ts)", 15L)
        .addExpr("DAYOFYEAR(ts)", 46L)
        .addExpr("DAYOFWEEK(ts)", 7L)
        .addExpr("HOUR(ts)", 11L)
        .addExpr("MINUTE(ts)", 35L)
        .addExpr("SECOND(ts)", 26L)
        .addExpr("FLOOR(ts TO YEAR)", parseDate("1986-01-01 00:00:00"))
        .addExpr("CEIL(ts TO YEAR)", parseDate("1987-01-01 00:00:00"))
        ;
    checker.buildRunAndCheck();
  }

  @Test public void testDateTimeFunctions_currentTime() throws Exception {
    String sql = "SELECT "
        + "LOCALTIME as l,"
        + "LOCALTIMESTAMP as l1,"
        + "CURRENT_DATE as c1,"
        + "CURRENT_TIME as c2,"
        + "CURRENT_TIMESTAMP as c3"
        + " FROM PCOLLECTION"
        ;
    PCollection<BeamRow> rows = getTestPCollection().apply(
        BeamSql.simpleQuery(sql));
    PAssert.that(rows).satisfies(new Checker());
    pipeline.run();
  }

  private static class Checker implements SerializableFunction<Iterable<BeamRow>, Void> {
    @Override public Void apply(Iterable<BeamRow> input) {
      Iterator<BeamRow> iter = input.iterator();
      assertTrue(iter.hasNext());
      BeamRow row = iter.next();
        // LOCALTIME
      Date date = new Date();
      assertTrue(date.getTime() - row.getGregorianCalendar(0).getTime().getTime() < 1000);
      assertTrue(date.getTime() - row.getDate(1).getTime() < 1000);
      assertTrue(date.getTime() - row.getDate(2).getTime() < 1000);
      assertTrue(date.getTime() - row.getGregorianCalendar(3).getTime().getTime() < 1000);
      assertTrue(date.getTime() - row.getDate(4).getTime() < 1000);
      assertFalse(iter.hasNext());
      return null;
    }
  }
}
