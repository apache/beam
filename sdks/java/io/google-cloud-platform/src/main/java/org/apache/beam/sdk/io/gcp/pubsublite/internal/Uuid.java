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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;

/** A Uuid storable in a Pub/Sub Lite attribute. */
@DefaultCoder(UuidCoder.class)
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class Uuid {
  public static final String DEFAULT_ATTRIBUTE = "x-goog-pubsublite-dataflow-uuid";

  public abstract ByteString value();

  public static Uuid of(ByteString value) {
    return new AutoValue_Uuid(value);
  }

  public static Uuid random() {
    UUID uuid = UUID.randomUUID();
    ByteStringOutputStream output = new ByteStringOutputStream(16);
    DataOutputStream stream = new DataOutputStream(output);
    try {
      stream.writeLong(uuid.getMostSignificantBits());
      stream.writeLong(uuid.getLeastSignificantBits());
    } catch (IOException e) {
      throw new RuntimeException("Should never have an IOException since there is no io.", e);
    }
    // Encode to Base64 so the random UUIDs are valid if consumed from the Cloud Pub/Sub client.
    return Uuid.of(
        ByteString.copyFrom(
            Base64.getEncoder().encode(output.toByteString().asReadOnlyByteBuffer())));
  }
}
