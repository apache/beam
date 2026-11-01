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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SchemaProvider} for {@link SplunkWriteError} objects.
 *
 * <p>{@link SplunkWriteError} accessors don't follow the getter naming convention, so the schema
 * {@link org.apache.beam.sdk.schemas.AutoValueSchema} infers for it has no fields and its schema
 * coder drops every value. This provider maps the accessors to row fields by hand.
 */
public class SplunkWriteErrorSchemaProvider implements SchemaProvider {

  static final String STATUS_CODE = "statusCode";
  static final String STATUS_MESSAGE = "statusMessage";
  static final String PAYLOAD = "payload";

  static final Schema SCHEMA =
      Schema.builder()
          .addNullableField(STATUS_CODE, Schema.FieldType.INT32)
          .addNullableField(STATUS_MESSAGE, Schema.FieldType.STRING)
          .addNullableField(PAYLOAD, Schema.FieldType.STRING)
          .build();

  @Override
  public <T> @Nullable Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return SCHEMA;
  }

  @Override
  public <T> @Nullable SerializableFunction<T, Row> toRowFunction(
      TypeDescriptor<T> typeDescriptor) {
    return new ToRowFn<>();
  }

  @Override
  public <T> @Nullable SerializableFunction<Row, T> fromRowFunction(
      TypeDescriptor<T> typeDescriptor) {
    return new FromRowFn<>();
  }

  /** Converts a {@link SplunkWriteError} to a {@link Row} with {@link #SCHEMA}. */
  static Row toRow(SplunkWriteError error) {
    return Row.withSchema(SCHEMA)
        .addValue(error.statusCode())
        .addValue(error.statusMessage())
        .addValue(error.payload())
        .build();
  }

  /** Converts a {@link SplunkWriteError} to a {@link Row} with {@link #SCHEMA}. */
  private static class ToRowFn<T> implements SerializableFunction<T, Row> {
    @Override
    public Row apply(T input) {
      return toRow((SplunkWriteError) checkNotNull(input));
    }

    // Equal instances keep schema coders built from this provider equal.
    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof ToRowFn;
    }

    @Override
    public int hashCode() {
      return ToRowFn.class.hashCode();
    }
  }

  /** Converts a {@link Row} with {@link #SCHEMA} back to a {@link SplunkWriteError}. */
  private static class FromRowFn<T> implements SerializableFunction<Row, T> {
    @Override
    @SuppressWarnings("unchecked")
    public T apply(Row row) {
      SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();

      Integer statusCode = row.getInt32(STATUS_CODE);
      if (statusCode != null) {
        builder.withStatusCode(statusCode);
      }

      String statusMessage = row.getString(STATUS_MESSAGE);
      if (statusMessage != null) {
        builder.withStatusMessage(statusMessage);
      }

      String payload = row.getString(PAYLOAD);
      if (payload != null) {
        builder.withPayload(payload);
      }

      return (T) builder.create();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof FromRowFn;
    }

    @Override
    public int hashCode() {
      return FromRowFn.class.hashCode();
    }
  }
}
