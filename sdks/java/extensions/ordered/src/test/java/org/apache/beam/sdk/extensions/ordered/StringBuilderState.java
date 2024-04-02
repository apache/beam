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
package org.apache.beam.sdk.extensions.ordered;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.StringBuilderState.StringBuilderStateCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * State used for processing test events. Uses StringBuilder to accumulate the output.
 *
 * <p>Can be configured to produce output with certain frequency.
 */
@DefaultCoder(StringBuilderStateCoder.class)
class StringBuilderState implements MutableState<String, String> {

  public static final String BAD_VALUE = "throw exception if you see me";

  private int emissionFrequency = 1;
  private long currentlyEmittedElementNumber;

  private final StringBuilder state = new StringBuilder();

  StringBuilderState(String initialEvent, int emissionFrequency) {
    this(initialEvent, emissionFrequency, 0L);
  }

  StringBuilderState(
      String initialEvent, int emissionFrequency, long currentlyEmittedElementNumber) {
    this.emissionFrequency = emissionFrequency;
    this.currentlyEmittedElementNumber = currentlyEmittedElementNumber;
    try {
      mutate(initialEvent);
    } catch (Exception e) {
      // this shouldn't happen because the input should be pre-validated.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void mutate(String event) throws Exception {
    if (event.equals(BAD_VALUE)) {
      throw new Exception("Validation failed");
    }
    state.append(event);
  }

  @Override
  public String produceResult() {
    return currentlyEmittedElementNumber++ % emissionFrequency == 0 ? state.toString() : null;
  }

  @Override
  public String toString() {
    return state.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StringBuilderState)) {
      return false;
    }
    StringBuilderState that = (StringBuilderState) o;
    return emissionFrequency == that.emissionFrequency
        && currentlyEmittedElementNumber == that.currentlyEmittedElementNumber
        && state.toString().equals(that.state.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }

  /** Coder for the StringBuilderState. */
  static class StringBuilderStateCoder extends Coder<StringBuilderState> {

    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<Integer> INT_CODER = VarIntCoder.of();

    @Override
    public void encode(
        StringBuilderState value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws IOException {
      INT_CODER.encode(value.emissionFrequency, outStream);
      LONG_CODER.encode(value.currentlyEmittedElementNumber, outStream);
      STRING_CODER.encode(value.state.toString(), outStream);
    }

    @Override
    public StringBuilderState decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws IOException {
      int emissionFrequency = INT_CODER.decode(inStream);
      long currentlyEmittedElementNumber = LONG_CODER.decode(inStream);
      String decoded = STRING_CODER.decode(inStream);
      StringBuilderState result =
          new StringBuilderState(decoded, emissionFrequency, currentlyEmittedElementNumber);
      return result;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<
            ? extends
                @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>>
        getCoderArguments() {
      return ImmutableList.of();
    }

    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Object structuralValue(StringBuilderState value) {
      return super.structuralValue(value);
    }
  }
}
