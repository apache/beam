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
package org.apache.beam.sdk.io.fileschematransform;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Class for capturing {@link FileWriteSchemaTransformProvider} write errors. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FileWriteSchemaTransformError {

  static Builder builder() {
    return new AutoValue_FileWriteSchemaTransformError.Builder();
  }

  /** The processed {@link Row} within which the error occurred. */
  public abstract Row getFailedRow();

  /** The thrown error message. */
  public abstract String getErrorMessage();

  /** The {@link Instant} when the error occurred. */
  public abstract Instant getObservedTime();

  /** The full error stack trace. */
  public abstract String getStackTrace();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFailedRow(Row value);

    abstract Builder setErrorMessage(String value);

    abstract Builder setObservedTime(Instant value);

    abstract Builder setStackTrace(String value);

    abstract FileWriteSchemaTransformError build();
  }
}
