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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithoutTimeZone;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for query parameters in Beam SQL. */
public class BeamSqlDslParametersTest extends BeamSqlDslBase {

  @Test
  public void testPositionalParameters() {
    String sql = "SELECT f_int, f_string FROM PCOLLECTION WHERE f_int = ? AND f_string = ?";

    PCollection<Row> result =
        boundedInput1.apply(
            "testPositionalParameters",
            SqlTransform.query(sql).withPositionalParameters(Arrays.asList(1, "string_row1")));

    Row expectedRow =
        Row.withSchema(Schema.builder().addInt32Field("f_int").addStringField("f_string").build())
            .addValues(1, "string_row1")
            .build();

    PAssert.that(result).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  @Test
  public void testDateTimeParameters() {
    String sql =
        "SELECT f_int FROM PCOLLECTION WHERE f_date = ? AND f_time = ? AND f_datetime = ? AND f_timestamp = ?";

    PCollection<Row> result =
        boundedInput1.apply(
            "testDateTimeParameters",
            SqlTransform.query(sql)
                .withPositionalParameters(
                    Arrays.asList(
                        LocalDate.of(2017, 1, 1),
                        LocalTime.of(1, 1, 3),
                        LocalDateTime.of(2017, 1, 1, 1, 1, 3),
                        new Instant(parseTimestampWithoutTimeZone("2017-01-01 01:01:03")))));

    Row expectedRow =
        Row.withSchema(Schema.builder().addInt32Field("f_int").build()).addValues(1).build();

    PAssert.that(result).containsInAnyOrder(expectedRow);

    pipeline.run();
  }
}
