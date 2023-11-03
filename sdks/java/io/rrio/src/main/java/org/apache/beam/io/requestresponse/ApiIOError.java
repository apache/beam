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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.joda.time.Instant;

/** {@link ApiIOError} is a data class for storing details about an error. */
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ApiIOError {

  static Builder builder() {
    return new AutoValue_ApiIOError.Builder();
  }

  /** The encoded UTF-8 string representation of the related processed element. */
  public abstract String getEncodedElementAsUtfString();

  /** The observed timestamp of the error. */
  public abstract Instant getObservedTimestamp();

  /** The {@link Exception} message. */
  public abstract String getMessage();

  /** The {@link Exception} stack trace. */
  public abstract String getStackTrace();

  @AutoValue.Builder
  abstract static class Builder {

    public abstract Builder setEncodedElementAsUtfString(String value);

    public abstract Builder setObservedTimestamp(Instant value);

    public abstract Builder setMessage(String value);

    public abstract Builder setStackTrace(String value);

    abstract ApiIOError build();
  }
}
