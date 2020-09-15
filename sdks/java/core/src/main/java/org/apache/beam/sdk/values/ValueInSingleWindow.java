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
package org.apache.beam.sdk.values;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * An immutable tuple of value, timestamp, window, and pane.
 *
 * @param <T> the type of the value
 */
@AutoValue
@Internal
public abstract class ValueInSingleWindow<T> {
  /** Returns the value of this {@code ValueInSingleWindow}. */
  public abstract @Nullable T getValue();

  /** Returns the timestamp of this {@code ValueInSingleWindow}. */
  public abstract Instant getTimestamp();

  /** Returns the window of this {@code ValueInSingleWindow}. */
  public abstract BoundedWindow getWindow();

  /** Returns the pane of this {@code ValueInSingleWindow} in its window. */
  public abstract PaneInfo getPane();

  public static <T> ValueInSingleWindow<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return new AutoValue_ValueInSingleWindow<>(value, timestamp, window, paneInfo);
  }

  /** A coder for {@link ValueInSingleWindow}. */
  public static class Coder<T> extends StructuredCoder<ValueInSingleWindow<T>> {
    private final org.apache.beam.sdk.coders.Coder<T> valueCoder;
    private final org.apache.beam.sdk.coders.Coder<BoundedWindow> windowCoder;

    public static <T> Coder<T> of(
        org.apache.beam.sdk.coders.Coder<T> valueCoder,
        org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder) {
      return new Coder<>(valueCoder, windowCoder);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder(
        org.apache.beam.sdk.coders.Coder<T> valueCoder,
        org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder) {
      this.valueCoder = valueCoder;
      this.windowCoder = (org.apache.beam.sdk.coders.Coder) windowCoder;
    }

    @Override
    public void encode(ValueInSingleWindow<T> windowedElem, OutputStream outStream)
        throws IOException {
      encode(windowedElem, outStream, Context.NESTED);
    }

    @Override
    public void encode(ValueInSingleWindow<T> windowedElem, OutputStream outStream, Context context)
        throws IOException {
      InstantCoder.of().encode(windowedElem.getTimestamp(), outStream);
      windowCoder.encode(windowedElem.getWindow(), outStream);
      PaneInfo.PaneInfoCoder.INSTANCE.encode(windowedElem.getPane(), outStream);
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public ValueInSingleWindow<T> decode(InputStream inStream) throws IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public ValueInSingleWindow<T> decode(InputStream inStream, Context context) throws IOException {
      Instant timestamp = InstantCoder.of().decode(inStream);
      BoundedWindow window = windowCoder.decode(inStream);
      PaneInfo pane = PaneInfo.PaneInfoCoder.INSTANCE.decode(inStream);
      T value = valueCoder.decode(inStream, context);
      return new AutoValue_ValueInSingleWindow<>(value, timestamp, window, pane);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      // Coder arguments are coders for the type parameters of the coder - i.e. only T.
      return ImmutableList.of(valueCoder);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getComponents() {
      // Coder components are all inner coders that it uses - i.e. both T and BoundedWindow.
      return ImmutableList.of(valueCoder, windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      valueCoder.verifyDeterministic();
      windowCoder.verifyDeterministic();
    }
  }
}
