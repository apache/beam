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
package org.apache.beam.sdk.io.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link SplunkWriteError} objects. */
public class SplunkWriteErrorCoder extends AtomicCoder<SplunkWriteError> {

  private static final SplunkWriteErrorCoder SPLUNK_WRITE_ERROR_CODER = new SplunkWriteErrorCoder();

  private static final TypeDescriptor<SplunkWriteError> TYPE_DESCRIPTOR =
      new TypeDescriptor<SplunkWriteError>() {};
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> STRING_NULLABLE_CODER =
      NullableCoder.of(STRING_UTF_8_CODER);
  private static final NullableCoder<Integer> INTEGER_NULLABLE_CODER =
      NullableCoder.of(BigEndianIntegerCoder.of());

  /** Builds a {@link org.apache.beam.sdk.coders.Coder} for {@link SplunkWriteError} objects. */
  public static SplunkWriteErrorCoder of() {
    return SPLUNK_WRITE_ERROR_CODER;
  }

  @Override
  public void encode(SplunkWriteError value, OutputStream out) throws CoderException, IOException {
    INTEGER_NULLABLE_CODER.encode(value.statusCode(), out);
    STRING_NULLABLE_CODER.encode(value.statusMessage(), out);
    STRING_NULLABLE_CODER.encode(value.payload(), out);
  }

  @Override
  public SplunkWriteError decode(InputStream in) throws CoderException, IOException {

    SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();

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
  public TypeDescriptor<SplunkWriteError> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "SplunkWriteError can hold arbitrary instances, which may be non-deterministic.");
  }
}
