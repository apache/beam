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
package org.apache.beam.sdk.schemas.transforms.providers;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.values.Row;

@AutoValue
public abstract class ErrorHandling {
  @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
  public abstract String getOutput();

  public static Builder builder() {
    return new AutoValue_ErrorHandling.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOutput(String output);

    public abstract ErrorHandling build();
  }

  public static boolean hasOutput(@Nullable ErrorHandling errorHandling) {
    return getOutputOrNull(errorHandling) != null;
  }

  public static @Nullable String getOutputOrNull(@Nullable ErrorHandling errorHandling) {
    return errorHandling == null ? null : errorHandling.getOutput();
  }

  public static Schema errorSchema(Schema inputSchema) {
    return Schema.of(
        Schema.Field.of("failed_row", Schema.FieldType.row(inputSchema)),
        Schema.Field.of("error_message", Schema.FieldType.STRING));
  }

  public static Schema errorSchemaBytes() {
    return Schema.of(
        Schema.Field.of("failed_row", Schema.FieldType.BYTES),
        Schema.Field.of("error_message", Schema.FieldType.STRING));
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  public static Row errorRecord(Schema errorSchema, Row inputRow, Throwable th) {
    return Row.withSchema(errorSchema)
        .withFieldValue("failed_row", inputRow)
        .withFieldValue("error_message", th.getMessage())
        .build();
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  public static Row errorRecord(Schema errorSchema, byte[] inputBytes, Throwable th) {
    return Row.withSchema(errorSchema)
        .withFieldValue("failed_row", inputBytes)
        .withFieldValue("error_message", th.getMessage())
        .build();
  }
}
