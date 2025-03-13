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
package org.apache.beam.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * A {@link TimestampPrefixingWindowCoder} wraps arbitrary user custom window coder. While encoding
 * the custom window type, it extracts the maxTimestamp(inclusive) of the window and prefix it to
 * the encoded bytes of the window using the user custom window coder.
 *
 * @param <T> The custom window type.
 */
public class TimestampPrefixingWindowCoder<T extends BoundedWindow> extends StructuredCoder<T> {
  private final Coder<T> windowCoder;

  public static <T extends BoundedWindow> TimestampPrefixingWindowCoder<T> of(
      Coder<T> windowCoder) {
    return new TimestampPrefixingWindowCoder<>(windowCoder);
  }

  TimestampPrefixingWindowCoder(Coder<T> windowCoder) {
    this.windowCoder = windowCoder;
  }

  public Coder<T> getWindowCoder() {
    return windowCoder;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("Cannot encode null window");
    }
    InstantCoder.of().encode(value.maxTimestamp(), outStream);
    windowCoder.encode(value, outStream);
  }

  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    InstantCoder.of().decode(inStream);
    return windowCoder.decode(inStream);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Lists.newArrayList(windowCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    windowCoder.verifyDeterministic();
  }

  @Override
  public boolean consistentWithEquals() {
    return windowCoder.consistentWithEquals();
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(T value) {
    return windowCoder.isRegisterByteSizeObserverCheap(value);
  }

  @Override
  public void registerByteSizeObserver(T value, ElementByteSizeObserver observer) throws Exception {
    InstantCoder.of().registerByteSizeObserver(value.maxTimestamp(), observer);
    windowCoder.registerByteSizeObserver(value, observer);
  }
}
