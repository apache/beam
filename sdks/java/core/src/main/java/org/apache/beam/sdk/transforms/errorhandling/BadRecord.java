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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class BadRecord implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BadRecord.class);

  /** Information about the record that failed. */
  public abstract Record getRecord();

  /** Information about why the record failed. */
  public abstract Failure getFailure();

  public static Builder builder() {
    return new AutoValue_BadRecord.Builder();
  }

  public static Coder<BadRecord> getCoder(Pipeline pipeline) {
    try {
      SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
      return SchemaCoder.of(
          schemaRegistry.getSchema(BadRecord.class),
          TypeDescriptor.of(BadRecord.class),
          schemaRegistry.getToRowFunction(BadRecord.class),
          schemaRegistry.getFromRowFunction(BadRecord.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  public static <RecordT> BadRecord fromExceptionInformation(
      RecordT record,
      @Nullable Coder<RecordT> coder,
      @Nullable Exception exception,
      String description)
      throws IOException {
    Preconditions.checkArgumentNotNull(record);

    // Build up record information
    BadRecord.Record.Builder recordBuilder = Record.builder();
    recordBuilder.addHumanReadableJson(record).addCoderAndEncodedRecord(coder, record);

    // Build up failure information
    BadRecord.Failure.Builder failureBuilder = Failure.builder().setDescription(description);

    // It's possible for us to want to handle an error scenario where no actual exception object
    // exists
    if (exception != null) {
      failureBuilder.setException(exception.toString()).addExceptionStackTrace(exception);
    }

    return BadRecord.builder()
        .setRecord(recordBuilder.build())
        .setFailure(failureBuilder.build())
        .build();
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
    public abstract @Nullable String getHumanReadableJsonRecord();

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

      public abstract Builder setHumanReadableJsonRecord(@Nullable String jsonRecord);

      public Builder addHumanReadableJson(Object record) {
        ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
          this.setHumanReadableJsonRecord(objectWriter.writeValueAsString(record));
        } catch (Exception e) {
          LOG.error(
              "Unable to serialize record as JSON. Human readable record attempted via .toString",
              e);
          try {
            this.setHumanReadableJsonRecord(record.toString());
          } catch (Exception e2) {
            LOG.error(
                "Unable to serialize record via .toString. Human readable record will be null", e2);
          }
        }
        return this;
      }

      @SuppressWarnings("mutable")
      public abstract Builder setEncodedRecord(byte @Nullable [] encodedRecord);

      public abstract Builder setCoder(@Nullable String coder);

      public <T> Builder addCoderAndEncodedRecord(@Nullable Coder<T> coder, T record) {
        // We will sometimes not have a coder for a failing record, for example if it has already
        // been
        // modified within the dofn.
        if (coder != null) {
          this.setCoder(coder.toString());
          try {
            this.setEncodedRecord(CoderUtils.encodeToByteArray(coder, record));
          } catch (IOException e) {
            LOG.error(
                "Unable to encode failing record using provided coder."
                    + " BadRecord will be published without encoded bytes",
                e);
          }
        }
        return this;
      }

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

    public static Builder builder() {
      return new AutoValue_BadRecord_Failure.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setException(@Nullable String exception);

      public abstract Builder setExceptionStacktrace(@Nullable String stacktrace);

      public Builder addExceptionStackTrace(Exception exception) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(stream, false, StandardCharsets.UTF_8.name());
        exception.printStackTrace(printStream);
        printStream.close();

        this.setExceptionStacktrace(new String(stream.toByteArray(), StandardCharsets.UTF_8));
        return this;
      }

      public abstract Builder setDescription(String description);

      public abstract Failure build();
    }
  }
}
