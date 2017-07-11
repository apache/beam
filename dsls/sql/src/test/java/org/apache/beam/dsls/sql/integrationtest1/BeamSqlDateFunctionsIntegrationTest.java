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

package org.apache.beam.dsls.sql.integrationtest1;

import java.util.Date;
import java.util.Iterator;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration test for date functions.
 */
public class BeamSqlDateFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test public void testDateTimeFunctions() throws Exception {
    String sql = "SELECT "
        + "EXTRACT(YEAR FROM ts) as ex,"
        + "YEAR(ts) as y,"
        + "QUARTER(ts) as q,"
        + "MONTH(ts) as m,"
        + "WEEK(ts) as w,"
        + "DAYOFMONTH(ts) as d,"
        + "DAYOFYEAR(ts) as d1,"
        + "DAYOFWEEK(ts) as d2,"
        + "HOUR(ts) as h,"
        + "MINUTE(ts) as m1,"
        + "SECOND(ts) as s, "
        + "FLOOR(ts TO YEAR) as f,"
        + "CEIL(ts TO YEAR) as c, "
        + "LOCALTIME as l,"
        + "LOCALTIMESTAMP as l1,"
        + "CURRENT_DATE as c1,"
        + "CURRENT_TIME as c2,"
        + "CURRENT_TIMESTAMP as c3"
        + " FROM PCOLLECTION"
        ;
    PCollection<BeamSqlRow> rows = getTestPCollection().apply(
        BeamSql.simpleQuery(sql));
    PAssert.that(rows).satisfies(new Checker());
    pipeline.run();
  }

  private static class Checker implements SerializableFunction<Iterable<BeamSqlRow>, Void> {
    @Override public Void apply(Iterable<BeamSqlRow> input) {
      Iterator<BeamSqlRow> iter = input.iterator();
      while (iter.hasNext()) {
        BeamSqlRow row = iter.next();
        Assert.assertEquals(1986L, row.getLong(0));
        Assert.assertEquals(1986L, row.getLong(1));
        Assert.assertEquals(1L, row.getLong(2));
        Assert.assertEquals(2L, row.getLong(3));
        Assert.assertEquals(7L, row.getLong(4));
        Assert.assertEquals(15L, row.getLong(5));
        Assert.assertEquals(46L, row.getLong(6));
        Assert.assertEquals(7L, row.getLong(7));
        Assert.assertEquals(11L, row.getLong(8));
        Assert.assertEquals(35L, row.getLong(9));
        Assert.assertEquals(26L, row.getLong(10));
        Assert.assertEquals(parseDate("1986-01-01 00:00:00"), row.getDate(11));
        Assert.assertEquals(parseDate("1987-01-01 00:00:00"), row.getDate(12));

        // LOCALTIME
        Date date = new Date();
        Assert.assertTrue(date.getTime() - row.getGregorianCalendar(13).getTime().getTime() < 1000);
        Assert.assertTrue(date.getTime() - row.getDate(14).getTime() < 1000);
        Assert.assertTrue(date.getTime() - row.getDate(15).getTime() < 1000);
        Assert.assertTrue(date.getTime() - row.getGregorianCalendar(16).getTime().getTime() < 1000);
        Assert.assertTrue(date.getTime() - row.getDate(17).getTime() < 1000);
      }
      return null;
    }
  }
}
