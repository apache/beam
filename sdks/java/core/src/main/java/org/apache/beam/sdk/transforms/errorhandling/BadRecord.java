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
package org.apache.beam.sdk.transforms.errorhandling;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class BadRecord implements Serializable {

  /** Information about the record that failed. */
  public abstract Record getRecord();

  /** Information about why the record failed. */
  public abstract Failure getFailure();

  public static Builder builder() {
    return new AutoValue_BadRecord.Builder();
  }
  public static Coder<BadRecord> getCoder(Pipeline pipeline){
    try {
      SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
      return
          SchemaCoder.of(
              schemaRegistry.getSchema(BadRecord.class),
              TypeDescriptor.of(BadRecord.class),
              schemaRegistry.getToRowFunction(BadRecord.class),
              schemaRegistry.getFromRowFunction(BadRecord.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRecord(Record record);

    public abstract Builder setFailure(Failure error);

    public abstract BadRecord build();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Record implements Serializable {

    /** The failing record, encoded as JSON. Will be null if serialization as JSON fails. */
    public abstract @Nullable String getJsonRecord();

    /**
     * Nullable to account for failing to encode, or if there is no coder for the record at the time
     * of failure.
     */
    @SuppressWarnings("mutable")
    public abstract byte @Nullable [] getEncodedRecord();

    /** The coder for the record, or null if there is no coder. */
    public abstract @Nullable String getCoder();

    public static Builder builder() {
      return new AutoValue_BadRecord_Record.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setJsonRecord(@Nullable String jsonRecord);

      @SuppressWarnings("mutable")
      public abstract Builder setEncodedRecord(byte @Nullable [] encodedRecord);

      public abstract Builder setCoder(@Nullable String coder);

      public abstract Record build();
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Failure implements Serializable {

    /** The exception itself, e.g. IOException. Null if there is a failure without an exception. */
    public abstract @Nullable String getException();

    /** The full stacktrace. Null if there is a failure without an exception. */
    public abstract @Nullable String getExceptionStacktrace();

    /** The description of what was being attempted when the failure occurred. */
    public abstract String getDescription();

    /** The particular sub-transform that failed. */
    public abstract String getFailingTransform();

    public static Builder builder() {
      return new AutoValue_BadRecord_Failure.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setException(@Nullable String exception);

      public abstract Builder setExceptionStacktrace(@Nullable String stacktrace);

      public abstract Builder setDescription(String description);

      public abstract Builder setFailingTransform(String failingTransform);

      public abstract Failure build();
    }
  }
}
