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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.ZetaSQLValue.ValueProto.Datetime;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.values.Row;

@Internal
public class ZetaSqlTypes {
  /** A Beam LogicalType corresponding to ZetaSQL DATETIME type. */
  public static class DatetimeLogicalType implements LogicalType<Datetime, Row> {
    public static final String IDENTIFIER = "DatetimeLogicalType";
    public static final String SECONDS_FIELD_NAME = "bit_field_datetime_seconds";
    public static final String NANOS_FIELD_NAME = "nanos";

    public static final Schema DATETIME_SCHEMA =
        Schema.builder().addInt64Field(SECONDS_FIELD_NAME).addInt32Field(NANOS_FIELD_NAME).build();

    @Override
    public String getIdentifier() {
      return IDENTIFIER;
    }

    @Override
    public FieldType getArgumentType() {
      return FieldType.STRING; // Not used
    }

    @Override
    public FieldType getBaseType() {
      return FieldType.row(DATETIME_SCHEMA);
    }

    @Override
    public Row toBaseType(Datetime input) {
      return Row.withSchema(DATETIME_SCHEMA)
          .addValues(input.getBitFieldDatetimeSeconds(), input.getNanos())
          .build();
    }

    @Override
    public Datetime toInputType(Row base) {
      return Datetime.newBuilder()
          .setBitFieldDatetimeSeconds(base.getInt64(SECONDS_FIELD_NAME))
          .setNanos(base.getInt32(NANOS_FIELD_NAME))
          .build();
    }
  }
}
