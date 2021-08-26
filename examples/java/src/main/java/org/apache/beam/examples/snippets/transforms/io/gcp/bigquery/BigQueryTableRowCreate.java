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
package org.apache.beam.examples.snippets.transforms.io.gcp.bigquery;

// [START bigquery_table_row_create]

import com.google.api.services.bigquery.model.TableRow;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BigQueryTableRowCreate {
  public static TableRow createTableRow() {
    TableRow row =
        new TableRow()
            // To learn more about BigQuery data types:
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
            .set("string_field", "UTF-8 strings are supported! 🌱🌳🌍")
            .set("int64_field", 432)
            .set("float64_field", 3.141592653589793)
            .set("numeric_field", new BigDecimal("1234.56").toString())
            .set("bool_field", true)
            .set(
                "bytes_field",
                Base64.getEncoder()
                    .encodeToString("UTF-8 byte string 🌱🌳🌍".getBytes(StandardCharsets.UTF_8)))

            // To learn more about date formatting:
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html
            .set("date_field", LocalDate.parse("2020-03-19").toString()) // ISO_LOCAL_DATE
            .set(
                "datetime_field",
                LocalDateTime.parse("2020-03-19T20:41:25.123").toString()) // ISO_LOCAL_DATE_TIME
            .set("time_field", LocalTime.parse("20:41:25.123").toString()) // ISO_LOCAL_TIME
            .set(
                "timestamp_field",
                Instant.parse("2020-03-20T03:41:42.123Z").toString()) // ISO_INSTANT

            // To learn more about the geography Well-Known Text (WKT) format:
            // https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
            .set("geography_field", "POINT(30 10)")

            // An array has its mode set to REPEATED.
            .set("array_field", Arrays.asList(1, 2, 3, 4))

            // Any class can be written as a STRUCT as long as all the fields in the
            // schema are present and they are encoded correctly as BigQuery types.
            .set(
                "struct_field",
                Stream.of(
                        new SimpleEntry<>("string_value", "Text 🌱🌳🌍"),
                        new SimpleEntry<>("int64_value", "42"))
                    .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));
    return row;
  }
}
// [END bigquery_table_row_create]
