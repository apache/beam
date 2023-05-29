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
package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
public abstract class ConversionError<SourceT> implements Serializable {

  private static final AutoValueSchema SCHEMA_PROVIDER = new AutoValueSchema();

  public static <SourceT> @NonNull Schema getSchema() {
    TypeDescriptor<ConversionError<SourceT>> type =
        new TypeDescriptor<ConversionError<SourceT>>() {};
    return checkStateNotNull(SCHEMA_PROVIDER.schemaFor(type));
  }

  public static <SourceT> SerializableFunction<ConversionError<SourceT>, Row> toRowFn() {
    TypeDescriptor<ConversionError<SourceT>> type =
        new TypeDescriptor<ConversionError<SourceT>>() {};
    return SCHEMA_PROVIDER.toRowFunction(type);
  }

  public static <SourceT> Builder<SourceT> builder() {
    return new AutoValue_ConversionError.Builder<>();
  }

  public abstract Instant getObservedTime();

  public abstract SourceT getSource();

  public abstract String getMessage();

  public abstract String getStackTrace();

  @AutoValue.Builder
  public abstract static class Builder<SourceT> {

    public abstract Builder<SourceT> setObservedTime(Instant value);

    public abstract Builder<SourceT> setSource(SourceT value);

    public abstract Builder<SourceT> setMessage(String value);

    public abstract Builder<SourceT> setStackTrace(String value);

    public abstract ConversionError<SourceT> build();
  }
}
