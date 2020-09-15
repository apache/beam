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
package org.apache.beam.sdk.io.aws2.sns;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Custom Coder for WrappedSnsResponse. */
class SnsResponseCoder<T> extends StructuredCoder<SnsResponse<T>> {

  private final Coder<T> elementCoder;
  private static final VarIntCoder STATUS_CODE_CODER = VarIntCoder.of();
  private static final StringUtf8Coder STATUS_TEXT_CODER = StringUtf8Coder.of();

  public SnsResponseCoder(Coder<T> elementCoder) {
    this.elementCoder = elementCoder;
  }

  static <T> SnsResponseCoder<T> of(Coder<T> elementCoder) {
    return new SnsResponseCoder<>(elementCoder);
  }

  @Override
  public void encode(SnsResponse<T> value, OutputStream outStream) throws IOException {
    T element = value.element();
    elementCoder.encode(element, outStream);

    OptionalInt statusCode = value.statusCode();
    if (statusCode.isPresent()) {
      BooleanCoder.of().encode(Boolean.TRUE, outStream);
      STATUS_CODE_CODER.encode(statusCode.getAsInt(), outStream);
    } else {
      BooleanCoder.of().encode(Boolean.FALSE, outStream);
    }

    Optional<String> statusText = value.statusText();
    if (statusText.isPresent()) {
      BooleanCoder.of().encode(Boolean.TRUE, outStream);
      STATUS_TEXT_CODER.encode(statusText.get(), outStream);
    } else {
      BooleanCoder.of().encode(Boolean.FALSE, outStream);
    }
  }

  @Override
  public SnsResponse<T> decode(InputStream inStream) throws IOException {
    T element = elementCoder.decode(inStream);

    OptionalInt statusCode = OptionalInt.empty();
    if (BooleanCoder.of().decode(inStream)) {
      statusCode = OptionalInt.of(STATUS_CODE_CODER.decode(inStream));
    }

    Optional<String> statusText = Optional.empty();
    if (BooleanCoder.of().decode(inStream)) {
      statusText = Optional.of(STATUS_TEXT_CODER.decode(inStream));
    }
    return SnsResponse.create(element, statusCode, statusText);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(elementCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    elementCoder.verifyDeterministic();
  }
}
