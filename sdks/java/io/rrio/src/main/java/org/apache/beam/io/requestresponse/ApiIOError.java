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
package org.apache.beam.io.requestresponse;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;

/** {@link ApiIOError} is a data class for storing details about an error. */
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ApiIOError {

  /**
   * Instantiate an {@link ApiIOError} from an {@link ErrorT} {@link T} element. The {@link T}
   * element is converted to a string by calling {@link Object#toString()}.
   */
  static <T, ErrorT extends Exception> ApiIOError of(ErrorT e, T element) {
    String request = "";
    if (element != null) {
      request = element.toString();
    }

    return ApiIOError.builder()
        .setRequestAsString(request)
        .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
        .setObservedTimestamp(Instant.now())
        .setStackTrace(Throwables.getStackTraceAsString(e))
        .build();
  }

  static Builder builder() {
    return new AutoValue_ApiIOError.Builder();
  }

  /** The string representation of the request associated with the error. */
  public abstract String getRequestAsString();

  /** The observed timestamp of the error. */
  public abstract Instant getObservedTimestamp();

  /** The {@link Exception} message. */
  public abstract String getMessage();

  /** The {@link Exception} stack trace. */
  public abstract String getStackTrace();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setRequestAsString(String value);

    abstract Builder setObservedTimestamp(Instant value);

    abstract Builder setMessage(String value);

    abstract Builder setStackTrace(String value);

    abstract ApiIOError build();
  }
}
