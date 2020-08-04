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
package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.joda.time.Instant;

public class HealthcareIOErrorCoder<T> extends CustomCoder<HealthcareIOError<T>> {
  private final Coder<T> originalCoder;
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final NullableCoder<Integer> INTEGER_CODER =
      NullableCoder.of(TextualIntegerCoder.of());
  private static final NullableCoder<Instant> INSTANT_CODER = NullableCoder.of(InstantCoder.of());

  HealthcareIOErrorCoder(Coder<T> originalCoder) {
    this.originalCoder = NullableCoder.of(originalCoder);
  }

  public static <T> HealthcareIOErrorCoder<T> of(Coder<T> originalCoder) {
    return new HealthcareIOErrorCoder<>(originalCoder);
  }

  @Override
  public void encode(HealthcareIOError<T> value, OutputStream outStream) throws IOException {

    originalCoder.encode(value.getDataResource(), outStream);

    STRING_CODER.encode(value.getErrorMessage(), outStream);
    STRING_CODER.encode(value.getStackTrace(), outStream);
    INSTANT_CODER.encode(value.getObservedTime(), outStream);
    INTEGER_CODER.encode(value.getStatusCode(), outStream);
  }

  @Override
  public HealthcareIOError<T> decode(InputStream inStream) throws IOException {
    T dataResource = originalCoder.decode(inStream);
    String errorMessage = STRING_CODER.decode(inStream);
    String stackTrace = STRING_CODER.decode(inStream);
    Instant observedTime = INSTANT_CODER.decode(inStream);
    Integer statusCode = INTEGER_CODER.decode(inStream);
    return new HealthcareIOError<>(
        dataResource, errorMessage, stackTrace, observedTime, statusCode);
  }
}
