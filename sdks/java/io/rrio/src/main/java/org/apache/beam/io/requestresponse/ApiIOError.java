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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

/** {@link ApiIOError} is a data class for storing details about an error. */
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ApiIOError {

  static <T, ErrorT extends Exception> ApiIOError of(
      @NonNull ErrorT e, @NonNull Coder<T> coder, @NonNull T element) throws CoderException {

    String encodedElement = CoderUtils.encodeToBase64(coder, element);

    return ApiIOError.builder()
        .setEncodedElementAsBase64String(encodedElement)
        .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
        .setObservedTimestamp(Instant.now())
        .setStackTrace(Throwables.getStackTraceAsString(e))
        .build();
  }

  static Builder builder() {
    return new AutoValue_ApiIOError.Builder();
  }

  /**
   * The Base64 string representation of the element encoded using {@link
   * org.apache.beam.sdk.coders.Coder#encode}.
   */
  public abstract String getEncodedElementAsBase64String();

  /**
   * Decodes the Base64 string representation of {@link #getEncodedElementAsBase64String()} into
   * original type using its {@link Coder}.
   */
  public <T> T decodeElement(Coder<T> coder) throws CoderException {
    return CoderUtils.decodeFromBase64(coder, getEncodedElementAsBase64String());
  }

  /** The observed timestamp of the error. */
  public abstract Instant getObservedTimestamp();

  /** The {@link Exception} message. */
  public abstract String getMessage();

  /** The {@link Exception} stack trace. */
  public abstract String getStackTrace();

  @AutoValue.Builder
  abstract static class Builder {

    public abstract Builder setEncodedElementAsBase64String(String value);

    public abstract Builder setObservedTimestamp(Instant value);

    public abstract Builder setMessage(String value);

    public abstract Builder setStackTrace(String value);

    abstract ApiIOError build();
  }
}
