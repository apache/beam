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

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_SECOND;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

/** Integration test for date functions. */
public class BeamSqlDateFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-10328")
  public void testDateTimeFunctions_currentTime() throws Exception {
    String sql =
        "SELECT "
            + "LOCALTIME as l,"
            + "LOCALTIMESTAMP as l1,"
            + "CURRENT_DATE as c1,"
            + "CURRENT_TIME as c2,"
            + "CURRENT_TIMESTAMP as c3"
            + " FROM PCOLLECTION";
    PCollection<Row> rows = getTestPCollection().apply(SqlTransform.query(sql));
    PAssert.that(rows).satisfies(new Checker());
    pipeline.run();
  }

  private static class Checker implements SerializableFunction<Iterable<Row>, Void> {
    @Override
    public Void apply(Iterable<Row> input) {
      Iterator<Row> iter = input.iterator();
      assertTrue(iter.hasNext());
      Row row = iter.next();
      // LOCALTIME
      DateTime date = DateTime.now();
      long millis = date.getMillis();
      int timeMillis = (int) (date.getMillis() % MILLIS_PER_DAY);

      // These asserts checks that various time casts are correct within 1 second.
      // We should pass in a deterministic clock for testing.

      // LOCALTIME
      assertTrue(
          timeMillis
                  - row.getLogicalTypeValue(0, LocalTime.class).toSecondOfDay() * MILLIS_PER_SECOND
              < MILLIS_PER_SECOND);
      assertTrue(
          timeMillis
                  - row.getLogicalTypeValue(0, LocalTime.class).toSecondOfDay() * MILLIS_PER_SECOND
              > -MILLIS_PER_SECOND);

      // LOCALTIMESTAMP
      assertTrue(millis - row.getDateTime(1).getMillis() < 1000);
      assertTrue(millis - row.getDateTime(1).getMillis() > -1000);

      // CURRENT_DATE
      assertTrue(
          millis - row.getLogicalTypeValue(2, LocalDate.class).toEpochDay() * MILLIS_PER_DAY
              < MILLIS_PER_DAY);
      assertTrue(
          millis - row.getLogicalTypeValue(2, LocalDate.class).toEpochDay() * MILLIS_PER_DAY
              > -MILLIS_PER_DAY);

      // CURRENT_TIME
      assertTrue(
          timeMillis
                  - row.getLogicalTypeValue(3, LocalTime.class).toSecondOfDay() * MILLIS_PER_SECOND
              < MILLIS_PER_SECOND);
      assertTrue(
          timeMillis
                  - row.getLogicalTypeValue(3, LocalTime.class).toSecondOfDay() * MILLIS_PER_SECOND
              > -MILLIS_PER_SECOND);

      // CURRENT_TIMESTAMP
      assertTrue(millis - row.getDateTime(4).getMillis() < 1000);
      assertTrue(millis - row.getDateTime(4).getMillis() > -1000);

      assertFalse(iter.hasNext());
      return null;
    }
  }
}
