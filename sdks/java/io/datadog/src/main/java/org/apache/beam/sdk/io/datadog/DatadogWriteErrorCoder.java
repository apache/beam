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
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link DatadogWriteError} objects. */
public class DatadogWriteErrorCoder extends AtomicCoder<DatadogWriteError> {

  private static final DatadogWriteErrorCoder DATADOG_WRITE_ERROR_CODER =
      new DatadogWriteErrorCoder();

  private static final TypeDescriptor<DatadogWriteError> TYPE_DESCRIPTOR =
      new TypeDescriptor<DatadogWriteError>() {};
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> STRING_NULLABLE_CODER =
      NullableCoder.of(STRING_UTF_8_CODER);
  private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER =
      NullableCoder.of(BigEndianIntegerCoder.of());

  public static DatadogWriteErrorCoder of() {
    return DATADOG_WRITE_ERROR_CODER;
  }

  @Override
  public void encode(DatadogWriteError value, OutputStream out) throws CoderException, IOException {
    INTEGER_NULLABLE_CODER.encode(value.statusCode(), out);
    STRING_NULLABLE_CODER.encode(value.statusMessage(), out);
    STRING_NULLABLE_CODER.encode(value.payload(), out);
  }

  @Override
  public DatadogWriteError decode(InputStream in) throws CoderException, IOException {

    DatadogWriteError.Builder builder = DatadogWriteError.newBuilder();

    Integer statusCode = INTEGER_NULLABLE_CODER.decode(in);
    if (statusCode != null) {
      builder.withStatusCode(statusCode);
    }

    String statusMessage = STRING_NULLABLE_CODER.decode(in);
    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }

    String payload = STRING_NULLABLE_CODER.decode(in);
    if (payload != null) {
      builder.withPayload(payload);
    }

    return builder.build();
  }

  @Override
  public TypeDescriptor<DatadogWriteError> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "DatadogWriteError can hold arbitrary instances, which may be non-deterministic.");
  }
}
