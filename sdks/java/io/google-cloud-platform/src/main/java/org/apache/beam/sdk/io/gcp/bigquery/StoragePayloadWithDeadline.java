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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class StoragePayloadWithDeadline {
  abstract StorageApiWritePayload getStoragePayload();

  abstract org.joda.time.Instant getDeadline();

  static StoragePayloadWithDeadline of(
      StorageApiWritePayload payload, org.joda.time.Instant deadline) {
    return new AutoValue_StoragePayloadWithDeadline(payload, deadline);
  }

  // Schemas give us a coder, however there are still some limitations to storing schema objects
  // inside of state
  // variables (mostly involving the Dataflow runner and update). Therefore we use a custom coder.
  static class Coder extends CustomCoder<StoragePayloadWithDeadline> {
    static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
    static final InstantCoder INSTANT_CODER = InstantCoder.of();
    static final NullableCoder<byte[]> NULLABLE_BYTE_ARRAY_CODER =
        NullableCoder.of(BYTE_ARRAY_CODER);
    static final NullableCoder<Instant> NULLABLE_INSTANT_CODER =
        NullableCoder.of(InstantCoder.of());

    static StoragePayloadWithDeadline.Coder of() {
      return new Coder();
    }

    @Override
    public void encode(StoragePayloadWithDeadline value, OutputStream outStream)
        throws CoderException, IOException {
      BYTE_ARRAY_CODER.encode(value.getStoragePayload().getPayload(), outStream);
      NULLABLE_BYTE_ARRAY_CODER.encode(
          value.getStoragePayload().getUnknownFieldsPayload(), outStream);
      NULLABLE_INSTANT_CODER.encode(value.getStoragePayload().getTimestamp(), outStream);
      NULLABLE_BYTE_ARRAY_CODER.encode(
          value.getStoragePayload().getFailsafeTableRowPayload(), outStream);
      NULLABLE_BYTE_ARRAY_CODER.encode(value.getStoragePayload().getSchemaHash(), outStream);
      INSTANT_CODER.encode(value.getDeadline(), outStream);
    }

    @Override
    public StoragePayloadWithDeadline decode(InputStream inStream)
        throws CoderException, IOException {
      byte[] innerPayload = BYTE_ARRAY_CODER.decode(inStream);
      byte @Nullable [] unknownFieldsPayload = NULLABLE_BYTE_ARRAY_CODER.decode(inStream);
      @Nullable Instant timestamp = NULLABLE_INSTANT_CODER.decode(inStream);
      byte @Nullable [] failsafeTableRowPayload = NULLABLE_BYTE_ARRAY_CODER.decode(inStream);
      byte @Nullable [] schemaHash = NULLABLE_BYTE_ARRAY_CODER.decode(inStream);
      Instant deadline = INSTANT_CODER.decode(inStream);

      StorageApiWritePayload payload =
          StorageApiWritePayload.of(
              innerPayload, timestamp, unknownFieldsPayload, failsafeTableRowPayload, schemaHash);
      return new AutoValue_StoragePayloadWithDeadline(payload, deadline);
    }
  }
}
