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

import java.time.Instant;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * A timestamp without a time-zone.
 *
 * <p>It represents an instant on the time-line.
 *
 * <p>Its input type is a {@link Instant}, and base type is a {@link Row} containing EpochSeconds
 * field and Nanos field. EpochSeconds field is a Long that represents incrementing count of seconds
 * from 1970-01-01T00:00:00Z. Nanos field is an Int that represents nanoseconds of second.
 */
public class Timestamp implements Schema.LogicalType<Instant, Row> {
  public static final String EPOCH_SECOND_FIELD_NAME = "EpochSeconds";
  public static final String NANO_FIELD_NAME = "Nanos";
  public static final Schema TIMESTAMP_SCHEMA =
      Schema.builder()
          .addInt64Field(EPOCH_SECOND_FIELD_NAME)
          .addInt32Field(NANO_FIELD_NAME)
          .build();

  @Override
  public String getIdentifier() {
    return "beam:logical_type:timestamp:v1";
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
    return Schema.FieldType.row(TIMESTAMP_SCHEMA);
  }

  @Override
  public Row toBaseType(Instant input) {
    return input == null
        ? null
        : Row.withSchema(TIMESTAMP_SCHEMA)
            .addValues(input.getEpochSecond(), input.getNano())
            .build();
  }

  @Override
  public Instant toInputType(Row base) {
    return base == null
        ? null
        : Instant.ofEpochSecond(
            base.getInt64(EPOCH_SECOND_FIELD_NAME), base.getInt32(NANO_FIELD_NAME));
  }
}
