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

import com.google.auto.value.AutoValue;
import java.time.Instant;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;

@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@AutoValue
@DefaultSchema(AutoValueSchema.class)
abstract class ExamplePojo {
  static Builder builder() {
    return new AutoValue_ExamplePojo.Builder();
  }

  abstract String getAString();

  abstract int getAnInteger();

  abstract Instant getDateTime();

  abstract double getDouble();

  abstract long getLong();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setAString(String aString);

    abstract Builder setAnInteger(int anInteger);

    abstract Builder setDateTime(Instant dateTime);

    abstract Builder setDouble(double doubleValue);

    abstract Builder setLong(long longValue);

    abstract ExamplePojo build();
  }
}
