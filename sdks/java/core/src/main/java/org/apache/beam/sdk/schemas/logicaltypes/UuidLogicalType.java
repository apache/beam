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

import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;

/** Base class for types representing UUID as two long values. */
public class UuidLogicalType implements Schema.LogicalType<UUID, Row> {
  public static final String IDENTIFIER = "beam:logical_type:uuid:v1";
  public static final String LEAST_SIGNIFICANT_BITS_FIELD_NAME = "LeastSignificantBits";
  public static final String MOST_SIGNIFICANT_BITS_FIELD_NAME = "MostSignificantBits";
  public static final Schema UUID_SCHEMA =
      Schema.builder()
          .addInt64Field(MOST_SIGNIFICANT_BITS_FIELD_NAME)
          .addInt64Field(LEAST_SIGNIFICANT_BITS_FIELD_NAME)
          .build();

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
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
  public FieldType getBaseType() {
    return Schema.FieldType.row(UUID_SCHEMA);
  }

  @Override
  public Row toBaseType(UUID input) {
    return Row.withSchema(UUID_SCHEMA)
        .addValues(input.getMostSignificantBits(), input.getLeastSignificantBits())
        .build();
  }

  @Override
  public UUID toInputType(Row base) {
    Long leastSignificantBitsValue = base.getInt64(LEAST_SIGNIFICANT_BITS_FIELD_NAME);
    Long mostSignificantBitsValue = base.getInt64(MOST_SIGNIFICANT_BITS_FIELD_NAME);
    if (leastSignificantBitsValue == null || mostSignificantBitsValue == null) {
      throw new IllegalStateException(
          String.format(
              "Most (%s) and least (%s) significant bits value shouldn't be null for UUID",
              mostSignificantBitsValue, leastSignificantBitsValue));
    }
    return new UUID(mostSignificantBitsValue, leastSignificantBitsValue);
  }
}
