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
package org.apache.beam.sdk.schemas.logicaltypes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * A datetime without a time-zone.
 *
 * <p>It cannot represent an instant on the time-line without additional information such as an
 * offset or time-zone.
 *
 * <p>Its input type is a {@link LocalDateTime}, and base type is a {@link Row} containing Date
 * field and Time field. Date field is a Long that represents incrementing count of days where day 0
 * is 1970-01-01 (ISO). Time field is a Long that represents a count of time in nanoseconds.
 */
public class DateTime implements Schema.LogicalType<LocalDateTime, Row> {
  private final Schema schema =
      Schema.builder().addInt64Field("Date").addInt64Field("Time").build();

  @Override
  public String getIdentifier() {
    return "beam:logical_type:datetime:v1";
  }

  // unused
  @Override
  public Schema.FieldType getArgumentType() {
    return Schema.FieldType.STRING;
  }

  // unused
  @Override
  public String getArgument() {
    return "";
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(schema);
  }

  @Override
  public Row toBaseType(LocalDateTime input) {
    return input == null
        ? null
        : Row.withSchema(schema)
            .addValues(input.toLocalDate().toEpochDay(), input.toLocalTime().toNanoOfDay())
            .build();
  }

  @Override
  public LocalDateTime toInputType(Row base) {
    return base == null
        ? null
        : LocalDateTime.of(
            LocalDate.ofEpochDay(base.getValue("Date")),
            LocalTime.ofNanoOfDay(base.getValue("Time")));
  }
}
