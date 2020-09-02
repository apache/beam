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
 * A timestamp represented as microseconds since the epoch.
 *
 * <p>WARNING: This logical type exists solely for interoperability with other type systems such as
 * SQL and other Beam SDKs. It should never be used in a native Java context where the {@code
 * java.time.Instant} instances it describes may have higher than microsecond precision. Ignoring
 * this will likely result in an {@code AssertionError} at pipeline execution time.
 *
 * <p>For a more faithful logical type to use with {@code java.time.Instant}, see {@link
 * NanosInstant}.
 */
public class MicrosInstant implements Schema.LogicalType<Instant, Row> {
  public static final String IDENTIFIER = "beam:logical_type:micros_instant:v1";
  private final Schema schema;

  public MicrosInstant() {
    this.schema = Schema.builder().addInt64Field("seconds").addInt32Field("micros").build();
  }

  @Override
  public Row toBaseType(Instant input) {
    if (input.getNano() % 1000 != 0) {
      throw new AssertionError(
          "micros_instant logical type encountered a Java "
              + "Instant with greater than microsecond precision.");
    }
    return Row.withSchema(schema).addValues(input.getEpochSecond(), input.getNano() / 1000).build();
  }

  @Override
  public Instant toInputType(Row row) {
    return Instant.ofEpochSecond(row.getInt64(0), row.getInt32(1) * 1000);
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema.FieldType getArgumentType() {
    return null;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(schema);
  }
}
