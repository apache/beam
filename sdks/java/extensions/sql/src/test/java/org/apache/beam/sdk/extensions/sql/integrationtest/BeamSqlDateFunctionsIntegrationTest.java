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

package org.apache.beam.sdk.extensions.sql.integrationtest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Test;

/**
 * Integration test for date functions.
 */
public class BeamSqlDateFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test public void testBasicDateTimeFunctions() throws Exception {
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

  @Test public void testDatetimePlusFunction() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("TIMESTAMPADD(SECOND, 3, TIMESTAMP '1984-04-19 01:02:03')",
            parseDate("1984-04-19 01:02:06"))
        .addExpr("TIMESTAMPADD(MINUTE, 3, TIMESTAMP '1984-04-19 01:02:03')",
            parseDate("1984-04-19 01:05:03"))
        .addExpr("TIMESTAMPADD(HOUR, 3, TIMESTAMP '1984-04-19 01:02:03')",
            parseDate("1984-04-19 04:02:03"))
        .addExpr("TIMESTAMPADD(DAY, 3, TIMESTAMP '1984-04-19 01:02:03')",
            parseDate("1984-04-22 01:02:03"))
        .addExpr("TIMESTAMPADD(MONTH, 2, TIMESTAMP '1984-01-19 01:02:03')",
            parseDate("1984-03-19 01:02:03"))
        .addExpr("TIMESTAMPADD(YEAR, 2, TIMESTAMP '1985-01-19 01:02:03')",
            parseDate("1987-01-19 01:02:03"))
        ;
    checker.buildRunAndCheck();
  }

  @Test public void testDatetimeInfixPlus() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '3' SECOND",
            parseDate("1984-01-19 01:02:06"))
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' MINUTE",
            parseDate("1984-01-19 01:04:03"))
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' HOUR",
            parseDate("1984-01-19 03:02:03"))
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' DAY",
            parseDate("1984-01-21 01:02:03"))
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' MONTH",
            parseDate("1984-03-19 01:02:03"))
        .addExpr("TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' YEAR",
            parseDate("1986-01-19 01:02:03"))
        ;
    checker.buildRunAndCheck();
  }

  @Test public void testTimestampDiff() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:01:58')", 0)
        .addExpr("TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:01:59')", 1)
        .addExpr("TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:02:00')", 2)

        .addExpr("TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:02:57')", 0)
        .addExpr("TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:02:58')", 1)
        .addExpr("TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 01:03:58')", 2)

        .addExpr("TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 02:01:57')", 0)
        .addExpr("TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 02:01:58')", 1)
        .addExpr("TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-19 03:01:58')", 2)

        .addExpr("TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-20 01:01:57')", 0)
        .addExpr("TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-20 01:01:58')", 1)
        .addExpr("TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
            + "TIMESTAMP '1984-04-21 01:01:58')", 2)

        .addExpr("TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
            + "TIMESTAMP '1984-02-19 01:01:57')", 0)
        .addExpr("TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
            + "TIMESTAMP '1984-02-19 01:01:58')", 1)
        .addExpr("TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
            + "TIMESTAMP '1984-03-19 01:01:58')", 2)

        .addExpr("TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
            + "TIMESTAMP '1982-01-19 01:01:57')", 0)
        .addExpr("TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
            + "TIMESTAMP '1982-01-19 01:01:58')", 1)
        .addExpr("TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
            + "TIMESTAMP '1983-01-19 01:01:58')", 2)

        .addExpr("TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
            + "TIMESTAMP '1980-01-19 01:01:58')", -1)
        .addExpr("TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
            + "TIMESTAMP '1979-01-19 01:01:58')", -2)
    ;
    checker.buildRunAndCheck();
  }

  @Test public void testTimestampMinusInterval() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '2' SECOND",
            parseDate("1984-04-19 01:01:56"))
        .addExpr("TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '1' MINUTE",
            parseDate("1984-04-19 01:00:58"))
        .addExpr("TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '4' HOUR",
            parseDate("1984-04-18 21:01:58"))
        .addExpr("TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '5' DAY",
            parseDate("1984-04-14 01:01:58"))
        .addExpr("TIMESTAMP '1984-01-19 01:01:58' - INTERVAL '2' MONTH",
            parseDate("1983-11-19 01:01:58"))
        .addExpr("TIMESTAMP '1984-01-19 01:01:58' - INTERVAL '1' YEAR",
            parseDate("1983-01-19 01:01:58"))
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
    PCollection<Row> rows = getTestPCollection().apply(
        BeamSql.query(sql));
    PAssert.that(rows).satisfies(new Checker());
    pipeline.run();
  }

  private static class Checker implements SerializableFunction<Iterable<Row>, Void> {
    @Override public Void apply(Iterable<Row> input) {
      Iterator<Row> iter = input.iterator();
      assertTrue(iter.hasNext());
      Row row = iter.next();
        // LOCALTIME
      DateTime date = DateTime.now();
      assertTrue(date.getMillis() - row.getDateTime(0).getMillis() < 1000);
      assertTrue(date.getMillis() - row.getDateTime(1).getMillis() < 1000);
      assertTrue(date.getMillis() - row.getDateTime(2).getMillis() < 1000);
      assertTrue(date.getMillis() - row.getDateTime(3).getMillis() < 1000);
      assertTrue(date.getMillis() - row.getDateTime(4).getMillis() < 1000);
      assertFalse(iter.hasNext());
      return null;
    }
  }
}
