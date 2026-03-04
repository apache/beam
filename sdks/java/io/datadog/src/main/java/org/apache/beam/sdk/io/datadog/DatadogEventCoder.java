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
package org.apache.beam.sdk.io.datadog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link DatadogEvent} objects. */
public class DatadogEventCoder extends AtomicCoder<DatadogEvent> {

  private static final DatadogEventCoder DATADOG_EVENT_CODER = new DatadogEventCoder();

  private static final TypeDescriptor<DatadogEvent> TYPE_DESCRIPTOR =
      new TypeDescriptor<DatadogEvent>() {};
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> STRING_NULLABLE_CODER =
      NullableCoder.of(STRING_UTF_8_CODER);

  public static DatadogEventCoder of() {
    return DATADOG_EVENT_CODER;
  }

  @Override
  public void encode(DatadogEvent value, OutputStream out) throws IOException {
    STRING_NULLABLE_CODER.encode(value.ddsource(), out);
    STRING_NULLABLE_CODER.encode(value.ddtags(), out);
    STRING_NULLABLE_CODER.encode(value.hostname(), out);
    STRING_NULLABLE_CODER.encode(value.service(), out);
    STRING_NULLABLE_CODER.encode(value.message(), out);
  }

  @Override
  public DatadogEvent decode(InputStream in) throws IOException {
    DatadogEvent.Builder builder = DatadogEvent.newBuilder();

    String source = STRING_NULLABLE_CODER.decode(in);
    if (source != null) {
      builder.withSource(source);
    }

    String tags = STRING_NULLABLE_CODER.decode(in);
    if (tags != null) {
      builder.withTags(tags);
    }

    String hostname = STRING_NULLABLE_CODER.decode(in);
    if (hostname != null) {
      builder.withHostname(hostname);
    }

    String service = STRING_NULLABLE_CODER.decode(in);
    if (service != null) {
      builder.withService(service);
    }

    String message = STRING_NULLABLE_CODER.decode(in);
    if (message != null) {
      builder.withMessage(message);
    }

    return builder.build();
  }

  @Override
  public TypeDescriptor<DatadogEvent> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "DatadogEvent can hold arbitrary instances, which may be non-deterministic.");
  }
}
