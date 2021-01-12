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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class for capturing errors that occur while writing {@link SplunkEvent} to Splunk's Http Event
 * Collector (HEC) end point.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SplunkWriteError {

  /** Provides a builder for creating {@link SplunkWriteError} objects. */
  public static Builder newBuilder() {
    return new AutoValue_SplunkWriteError.Builder();
  }

  public abstract @Nullable Integer statusCode();

  public abstract @Nullable String statusMessage();

  public abstract @Nullable String payload();

  /** A builder class for creating a {@link SplunkWriteError}. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setStatusCode(Integer statusCode);

    abstract Integer statusCode();

    abstract Builder setStatusMessage(String statusMessage);

    abstract Builder setPayload(String payload);

    abstract SplunkWriteError build();

    /**
     * Assigns a return status code to assist with debugging.
     *
     * @param statusCode status code to assign
     */
    public Builder withStatusCode(Integer statusCode) {
      checkNotNull(statusCode, "withStatusCode(statusCode) called with null input.");

      return setStatusCode(statusCode);
    }

    /**
     * Assigns a return status message to assist with debugging.
     *
     * @param statusMessage status message to assign
     */
    public Builder withStatusMessage(String statusMessage) {
      checkNotNull(statusMessage, "withStatusMessage(statusMessage) called with null input.");

      return setStatusMessage(statusMessage);
    }

    /**
     * Assigns the payload to be used for reprocessing.
     *
     * <p>This is generally the original payload sent to HEC.
     *
     * @param payload payload to assign
     */
    public Builder withPayload(String payload) {
      checkNotNull(payload, "withPayload(payload) called with null input.");

      return setPayload(payload);
    }

    /** Builds a {@link SplunkWriteError} object. */
    public SplunkWriteError create() {
      return build();
    }
  }
}
