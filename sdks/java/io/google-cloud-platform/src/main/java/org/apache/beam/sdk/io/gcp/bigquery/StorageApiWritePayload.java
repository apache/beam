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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.util.CoderUtils;
import org.joda.time.Instant;

/** Class used to wrap elements being sent to the Storage API sinks. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class StorageApiWritePayload {
  @SuppressWarnings("mutable")
  public abstract byte[] getPayload();

  @SuppressWarnings("mutable")
  public abstract @Nullable byte[] getUnknownFieldsPayload();

  public abstract @Nullable Instant getTimestamp();

  @SuppressWarnings("mutable")
  public abstract @Nullable byte[] getFailsafeTableRowPayload();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPayload(byte[] value);

    public abstract Builder setUnknownFieldsPayload(@Nullable byte[] value);

    public abstract Builder setFailsafeTableRowPayload(@Nullable byte[] value);

    public abstract Builder setTimestamp(@Nullable Instant value);

    public abstract StorageApiWritePayload build();
  }

  public abstract Builder toBuilder();

  @SuppressWarnings("nullness")
  static StorageApiWritePayload of(
      byte[] payload, @Nullable TableRow unknownFields, @Nullable TableRow failsafeTableRow)
      throws IOException {
    @Nullable byte[] unknownFieldsPayload = null;
    if (unknownFields != null) {
      unknownFieldsPayload = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), unknownFields);
    }
    @Nullable byte[] failsafeTableRowPayload = null;
    if (failsafeTableRow != null) {
      failsafeTableRowPayload =
          CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), failsafeTableRow);
    }
    return new AutoValue_StorageApiWritePayload.Builder()
        .setPayload(payload)
        .setUnknownFieldsPayload(unknownFieldsPayload)
        .setFailsafeTableRowPayload(failsafeTableRowPayload)
        .setTimestamp(null)
        .build();
  }

  public StorageApiWritePayload withTimestamp(Instant instant) {
    return toBuilder().setTimestamp(instant).build();
  }

  public @Memoized @Nullable TableRow getUnknownFields() throws IOException {
    @Nullable byte[] fields = getUnknownFieldsPayload();
    if (fields == null) {
      return null;
    }
    return CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), fields);
  }

  public @Memoized @Nullable TableRow getFailsafeTableRow() throws IOException {
    @Nullable byte[] failsafeTableRowPayload = getFailsafeTableRowPayload();
    if (failsafeTableRowPayload == null) {
      return null;
    }
    return CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), failsafeTableRowPayload);
  }
}
