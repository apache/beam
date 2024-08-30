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
package org.apache.beam.sdk.io.csv;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link CsvIOParseError} is a data class to store errors from CSV record processing. It is {@link
 * org.apache.beam.sdk.schemas.Schema} mapped for compatibility with writing to Beam Schema-aware
 * I/O connectors.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class CsvIOParseError {

  static Builder builder() {
    return new AutoValue_CsvIOParseError.Builder();
  }

  private static final SchemaProvider SCHEMA_PROVIDER = new AutoValueSchema();

  private static final TypeDescriptor<CsvIOParseError> TYPE =
      TypeDescriptor.of(CsvIOParseError.class);

  private static final Schema SCHEMA = checkStateNotNull(SCHEMA_PROVIDER.schemaFor(TYPE));

  private static final SerializableFunction<CsvIOParseError, Row> TO_ROW_FN =
      checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(TYPE));

  private static final SerializableFunction<Row, CsvIOParseError> FROM_ROW_FN =
      checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(TYPE));

  static final Coder<CsvIOParseError> CODER = SchemaCoder.of(SCHEMA, TYPE, TO_ROW_FN, FROM_ROW_FN);

  /** The caught {@link Exception#getMessage()}. */
  public abstract String getMessage();

  /**
   * The CSV record associated with the caught {@link Exception}. Annotated {@link Nullable} as not
   * all processing errors are associated with a CSV record.
   */
  public abstract @Nullable String getCsvRecord();

  /**
   * The filename associated with the caught {@link Exception}. Annotated {@link Nullable} as not
   * all processing errors are associated with a file.
   */
  public abstract @Nullable String getFilename();

  /** The date and time when the {@link Exception} occurred. */
  public abstract Instant getObservedTimestamp();

  /** The caught {@link Exception#getStackTrace()}. */
  public abstract String getStackTrace();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setMessage(String message);

    abstract Builder setCsvRecord(String csvRecord);

    abstract Builder setFilename(String filename);

    abstract Builder setObservedTimestamp(Instant observedTimestamp);

    abstract Builder setStackTrace(String stackTrace);

    public abstract CsvIOParseError build();
  }
}
