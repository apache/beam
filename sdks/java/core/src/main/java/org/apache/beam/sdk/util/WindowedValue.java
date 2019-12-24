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
package org.apache.beam.sdk.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/**
 * An immutable triple of value, timestamp, and windows.
 *
 * @param <T> the type of the value
 */
public abstract class WindowedValue<T> {

  /** Returns a {@code WindowedValue} with the given value, timestamp, and windows. */
  public static <T> WindowedValue<T> of(
      T value, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
    checkArgument(pane != null, "WindowedValue requires PaneInfo, but it was null");
    checkArgument(windows.size() > 0, "WindowedValue requires windows, but there were none");

    if (windows.size() == 1) {
      return of(value, timestamp, windows.iterator().next(), pane);
    } else {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows, pane);
    }
  }

  /** @deprecated for use only in compatibility with old broken code */
  @Deprecated
  static <T> WindowedValue<T> createWithoutValidation(
      T value, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
    if (windows.size() == 1) {
      return of(value, timestamp, windows.iterator().next(), pane);
    } else {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows, pane);
    }
  }

  /** Returns a {@code WindowedValue} with the given value, timestamp, and window. */
  public static <T> WindowedValue<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo pane) {
    checkArgument(pane != null, "WindowedValue requires PaneInfo, but it was null");

    boolean isGlobal = GlobalWindow.INSTANCE.equals(window);
    if (isGlobal && BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInGlobalWindow(value, pane);
    } else if (isGlobal) {
      return new TimestampedValueInGlobalWindow<>(value, timestamp, pane);
    } else {
      return new TimestampedValueInSingleWindow<>(value, timestamp, window, pane);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value in the {@link GlobalWindow} using the
   * default timestamp and pane.
   */
  public static <T> WindowedValue<T> valueInGlobalWindow(T value) {
    return new ValueInGlobalWindow<>(value, PaneInfo.NO_FIRING);
  }

  /**
   * Returns a {@code WindowedValue} with the given value in the {@link GlobalWindow} using the
   * default timestamp and the specified pane.
   */
  public static <T> WindowedValue<T> valueInGlobalWindow(T value, PaneInfo pane) {
    return new ValueInGlobalWindow<>(value, pane);
  }

  /**
   * Returns a {@code WindowedValue} with the given value and timestamp, {@code GlobalWindow} and
   * default pane.
   */
  public static <T> WindowedValue<T> timestampedValueInGlobalWindow(T value, Instant timestamp) {
    if (BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInGlobalWindow(value);
    } else {
      return new TimestampedValueInGlobalWindow<>(value, timestamp, PaneInfo.NO_FIRING);
    }
  }

  /**
   * Returns a new {@code WindowedValue} that is a copy of this one, but with a different value,
   * which may have a new type {@code NewT}.
   */
  public abstract <NewT> WindowedValue<NewT> withValue(NewT value);

  /** Returns the value of this {@code WindowedValue}. */
  public abstract T getValue();

  /** Returns the timestamp of this {@code WindowedValue}. */
  public abstract Instant getTimestamp();

  /** Returns the windows of this {@code WindowedValue}. */
  public abstract Collection<? extends BoundedWindow> getWindows();

  /** Returns the pane of this {@code WindowedValue} in its window. */
  public abstract PaneInfo getPane();

  /** Returns {@code true} if this WindowedValue has exactly one window. */
  public boolean isSingleWindowedValue() {
    return false;
  }

  /**
   * Returns a collection of {@link WindowedValue WindowedValues} identical to this one, except each
   * is in exactly one of the windows that this {@link WindowedValue} is in.
   */
  public Iterable<WindowedValue<T>> explodeWindows() {
    ImmutableList.Builder<WindowedValue<T>> windowedValues = ImmutableList.builder();
    for (BoundedWindow w : getWindows()) {
      windowedValues.add(of(getValue(), getTimestamp(), w, getPane()));
    }
    return windowedValues.build();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof WindowedValue)) {
      return false;
    } else {
      WindowedValue<?> that = (WindowedValue<?>) other;

      // Compare timestamps first as they are most likely to differ.
      // Also compare timestamps according to millis-since-epoch because otherwise expensive
      // comparisons are made on their Chronology objects.
      return this.getTimestamp().isEqual(that.getTimestamp())
          && Objects.equals(this.getValue(), that.getValue())
          && Objects.equals(this.getWindows(), that.getWindows())
          && Objects.equals(this.getPane(), that.getPane());
    }
  }

  @Override
  public int hashCode() {
    // Hash only the millis of the timestamp to be consistent with equals
    return Objects.hash(getValue(), getTimestamp().getMillis(), getWindows(), getPane());
  }

  @Override
  public abstract String toString();

  private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  /** A {@link WindowedValue} which holds exactly single window per value. */
  public interface SingleWindowedValue {

    /** @return the single window associated with this value. */
    BoundedWindow getWindow();
  }

  /**
   * An abstract superclass for implementations of {@link WindowedValue} that stores the value and
   * pane info.
   */
  private abstract static class SimpleWindowedValue<T> extends WindowedValue<T> {

    private final T value;
    private final PaneInfo pane;

    protected SimpleWindowedValue(T value, PaneInfo pane) {
      this.value = value;
      this.pane = checkNotNull(pane);
    }

    @Override
    public PaneInfo getPane() {
      return pane;
    }

    @Override
    public T getValue() {
      return value;
    }
  }

  /** The abstract superclass of WindowedValue representations where timestamp == MIN. */
  private abstract static class MinTimestampWindowedValue<T> extends SimpleWindowedValue<T> {
    public MinTimestampWindowedValue(T value, PaneInfo pane) {
      super(value, pane);
    }

    @Override
    public Instant getTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }

  /** The representation of a WindowedValue where timestamp == MIN and windows == {GlobalWindow}. */
  private static class ValueInGlobalWindow<T> extends MinTimestampWindowedValue<T>
      implements SingleWindowedValue {

    public ValueInGlobalWindow(T value, PaneInfo pane) {
      super(value, pane);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new ValueInGlobalWindow<>(newValue, getPane());
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean isSingleWindowedValue() {
      return true;
    }

    @Override
    public BoundedWindow getWindow() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ValueInGlobalWindow) {
        ValueInGlobalWindow<?> that = (ValueInGlobalWindow<?>) o;
        return Objects.equals(that.getPane(), this.getPane())
            && Objects.equals(that.getValue(), this.getValue());
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue(), getPane());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("pane", getPane())
          .toString();
    }
  }

  /** The abstract superclass of WindowedValue representations where timestamp is arbitrary. */
  private abstract static class TimestampedWindowedValue<T> extends SimpleWindowedValue<T> {
    private final Instant timestamp;

    public TimestampedWindowedValue(T value, Instant timestamp, PaneInfo pane) {
      super(value, pane);
      this.timestamp = checkNotNull(timestamp);
    }

    @Override
    public Instant getTimestamp() {
      return timestamp;
    }
  }

  /**
   * The representation of a WindowedValue where timestamp {@code >} MIN and windows ==
   * {GlobalWindow}.
   */
  private static class TimestampedValueInGlobalWindow<T> extends TimestampedWindowedValue<T>
      implements SingleWindowedValue {

    public TimestampedValueInGlobalWindow(T value, Instant timestamp, PaneInfo pane) {
      super(value, timestamp, pane);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInGlobalWindow<>(newValue, getTimestamp(), getPane());
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean isSingleWindowedValue() {
      return true;
    }

    @Override
    public BoundedWindow getWindow() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInGlobalWindow) {
        TimestampedValueInGlobalWindow<?> that = (TimestampedValueInGlobalWindow<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        return this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getPane(), this.getPane())
            && Objects.equals(that.getValue(), this.getValue());
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      return Objects.hash(getValue(), getPane(), getTimestamp().getMillis());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("pane", getPane())
          .toString();
    }
  }

  /**
   * The representation of a WindowedValue where timestamp is arbitrary and windows == a single
   * non-Global window.
   */
  private static class TimestampedValueInSingleWindow<T> extends TimestampedWindowedValue<T>
      implements SingleWindowedValue {

    private final BoundedWindow window;

    public TimestampedValueInSingleWindow(
        T value, Instant timestamp, BoundedWindow window, PaneInfo pane) {
      super(value, timestamp, pane);
      this.window = checkNotNull(window);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInSingleWindow<>(newValue, getTimestamp(), window, getPane());
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.singletonList(window);
    }

    @Override
    public boolean isSingleWindowedValue() {
      return true;
    }

    @Override
    public BoundedWindow getWindow() {
      return window;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInSingleWindow) {
        TimestampedValueInSingleWindow<?> that = (TimestampedValueInSingleWindow<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        return this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getValue(), this.getValue())
            && Objects.equals(that.getPane(), this.getPane())
            && Objects.equals(that.window, this.window);
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      return Objects.hash(getValue(), getTimestamp().getMillis(), getPane(), window);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("window", window)
          .add("pane", getPane())
          .toString();
    }
  }

  /** The representation of a WindowedValue, excluding the special cases captured above. */
  private static class TimestampedValueInMultipleWindows<T> extends TimestampedWindowedValue<T> {
    private Collection<? extends BoundedWindow> windows;

    public TimestampedValueInMultipleWindows(
        T value, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
      super(value, timestamp, pane);
      this.windows = checkNotNull(windows);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
      return new TimestampedValueInMultipleWindows<>(newValue, getTimestamp(), windows, getPane());
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInMultipleWindows) {
        TimestampedValueInMultipleWindows<?> that = (TimestampedValueInMultipleWindows<?>) o;
        // Compare timestamps first as they are most likely to differ.
        // Also compare timestamps according to millis-since-epoch because otherwise expensive
        // comparisons are made on their Chronology objects.
        if (this.getTimestamp().isEqual(that.getTimestamp())
            && Objects.equals(that.getValue(), this.getValue())
            && Objects.equals(that.getPane(), this.getPane())) {
          ensureWindowsAreASet();
          that.ensureWindowsAreASet();
          return that.windows.equals(this.windows);
        } else {
          return false;
        }
      } else {
        return super.equals(o);
      }
    }

    @Override
    public int hashCode() {
      // Hash only the millis of the timestamp to be consistent with equals
      ensureWindowsAreASet();
      return Objects.hash(getValue(), getTimestamp().getMillis(), getPane(), windows);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", getValue())
          .add("timestamp", getTimestamp())
          .add("windows", windows)
          .add("pane", getPane())
          .toString();
    }

    private void ensureWindowsAreASet() {
      if (!(windows instanceof Set)) {
        windows = new LinkedHashSet<>(windows);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the {@code Coder} to use for a {@code WindowedValue<T>}, using the given valueCoder and
   * windowCoder.
   */
  public static <T> FullWindowedValueCoder<T> getFullCoder(
      Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
    return FullWindowedValueCoder.of(valueCoder, windowCoder);
  }

  /** Returns the {@code ValueOnlyCoder} from the given valueCoder. */
  public static <T> ValueOnlyWindowedValueCoder<T> getValueOnlyCoder(Coder<T> valueCoder) {
    return ValueOnlyWindowedValueCoder.of(valueCoder);
  }

  /** Returns the {@code ParamWindowedValueCoder} from the given valueCoder. */
  public static <T> ParamWindowedValueCoder<T> getParamWindowedValueCoder(Coder<T> valueCoder) {
    return ParamWindowedValueCoder.of(valueCoder);
  }

  /** Abstract class for {@code WindowedValue} coder. */
  public abstract static class WindowedValueCoder<T> extends StructuredCoder<WindowedValue<T>> {
    final Coder<T> valueCoder;

    WindowedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    /** Returns the value coder. */
    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    /**
     * Returns a new {@code WindowedValueCoder} that is a copy of this one, but with a different
     * value coder.
     */
    public abstract <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder);
  }

  /** Coder for {@code WindowedValue}. */
  public static class FullWindowedValueCoder<T> extends WindowedValueCoder<T> {
    private final Coder<? extends BoundedWindow> windowCoder;
    // Precompute and cache the coder for a list of windows.
    private final Coder<Collection<? extends BoundedWindow>> windowsCoder;

    public static <T> FullWindowedValueCoder<T> of(
        Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      return new FullWindowedValueCoder<>(valueCoder, windowCoder);
    }

    FullWindowedValueCoder(Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      super(valueCoder);
      this.windowCoder = checkNotNull(windowCoder);
      // It's not possible to statically type-check correct use of the
      // windowCoder (we have to ensure externally that we only get
      // windows of the class handled by windowCoder), so type
      // windowsCoder in a way that makes encode() and decode() work
      // right, and cast the window type away here.
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Collection<? extends BoundedWindow>> collectionCoder =
          (Coder) CollectionCoder.of(this.windowCoder);
      this.windowsCoder = collectionCoder;
    }

    public Coder<? extends BoundedWindow> getWindowCoder() {
      return windowCoder;
    }

    public Coder<Collection<? extends BoundedWindow>> getWindowsCoder() {
      return windowsCoder;
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new FullWindowedValueCoder<>(valueCoder, windowCoder);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Context context)
        throws CoderException, IOException {
      InstantCoder.of().encode(windowedElem.getTimestamp(), outStream);
      windowsCoder.encode(windowedElem.getWindows(), outStream);
      PaneInfoCoder.INSTANCE.encode(windowedElem.getPane(), outStream);
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Instant timestamp = InstantCoder.of().decode(inStream);
      Collection<? extends BoundedWindow> windows = windowsCoder.decode(inStream);
      PaneInfo pane = PaneInfoCoder.INSTANCE.decode(inStream);
      T value = valueCoder.decode(inStream, context);

      // Because there are some remaining (incorrect) uses of WindowedValue with no windows,
      // we call this deprecated no-validation path when decoding
      return WindowedValue.createWithoutValidation(value, timestamp, windows, pane);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "FullWindowedValueCoder requires a deterministic valueCoder", valueCoder);
      verifyDeterministic(
          this, "FullWindowedValueCoder requires a deterministic windowCoder", windowCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      InstantCoder.of().registerByteSizeObserver(value.getTimestamp(), observer);
      windowsCoder.registerByteSizeObserver(value.getWindows(), observer);
      PaneInfoCoder.INSTANCE.registerByteSizeObserver(value.getPane(), observer);
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    /**
     * {@inheritDoc}.
     *
     * @return a singleton list containing the {@code valueCoder} of this {@link
     *     FullWindowedValueCoder}.
     */
    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      // The value type is the only generic type parameter exposed by this coder. The component
      // coders include the window coder as well
      return Collections.singletonList(valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Arrays.asList(valueCoder, windowCoder);
    }
  }

  /**
   * Coder for {@code WindowedValue}.
   *
   * <p>A {@code ValueOnlyWindowedValueCoder} only encodes and decodes the value. It drops timestamp
   * and windows for encoding, and uses defaults timestamp, and windows for decoding.
   *
   * @deprecated Use ParamWindowedValueCoder instead it is general purpose implementation of the
   *     same concept but makes timestamp, windows and pane info configurable.
   */
  @Deprecated
  public static class ValueOnlyWindowedValueCoder<T> extends WindowedValueCoder<T> {
    public static <T> ValueOnlyWindowedValueCoder<T> of(Coder<T> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    ValueOnlyWindowedValueCoder(Coder<T> valueCoder) {
      super(valueCoder);
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Context context)
        throws CoderException, IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      T value = valueCoder.decode(inStream, context);
      return WindowedValue.valueInGlobalWindow(value);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "ValueOnlyWindowedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }
  }

  /**
   * A parameterized Coder for {@code WindowedValue}.
   *
   * <p>A {@code ParamWindowedValueCoder} only encodes and decodes the value. It drops timestamp,
   * windows, and pane info during encoding, and uses the supplied parameterized timestamp, windows
   * and pane info values during decoding when reconstructing the windowed value.
   */
  public static class ParamWindowedValueCoder<T> extends FullWindowedValueCoder<T> {

    private static final long serialVersionUID = 1L;

    private transient Instant timestamp;
    private transient Collection<? extends BoundedWindow> windows;
    private transient PaneInfo pane;

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Returns the {@link ParamWindowedValueCoder} from the given valueCoder, windowCoder and the
     * supplied parameterized timestamp, windows and pane info for {@link WindowedValue}.
     */
    public static <T> ParamWindowedValueCoder<T> of(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      return new ParamWindowedValueCoder<>(valueCoder, windowCoder, timestamp, windows, pane);
    }

    /**
     * Returns the {@link ParamWindowedValueCoder} from the given valueCoder, windowCoder and the
     * supplied parameterized {@link BoundedWindow#TIMESTAMP_MIN_VALUE}, {@link #GLOBAL_WINDOWS} and
     * {@link PaneInfo#NO_FIRING} for {@link WindowedValue}.
     */
    public static <T> ParamWindowedValueCoder<T> of(
        Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      return ParamWindowedValueCoder.of(
          valueCoder,
          windowCoder,
          BoundedWindow.TIMESTAMP_MIN_VALUE,
          GLOBAL_WINDOWS,
          PaneInfo.NO_FIRING);
    }

    /**
     * Returns the {@link ParamWindowedValueCoder} from the given valueCoder, {@link
     * GlobalWindow.Coder#INSTANCE} and the supplied parameterized {@link
     * BoundedWindow#TIMESTAMP_MIN_VALUE}, {@link #GLOBAL_WINDOWS} and {@link PaneInfo#NO_FIRING}
     * for {@link WindowedValue}.
     */
    public static <T> ParamWindowedValueCoder<T> of(Coder<T> valueCoder) {
      return ParamWindowedValueCoder.of(valueCoder, GlobalWindow.Coder.INSTANCE);
    }

    ParamWindowedValueCoder(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      super(valueCoder, windowCoder);
      this.timestamp = timestamp;
      this.windows = windows;
      this.pane = pane;
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new ParamWindowedValueCoder<>(valueCoder, getWindowCoder(), timestamp, windows, pane);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream)
        throws CoderException, IOException {
      encode(windowedElem, outStream, Context.NESTED);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Context context)
        throws CoderException, IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      T value = valueCoder.decode(inStream, context);
      return WindowedValue.of(value, this.timestamp, this.windows, this.pane);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "ParamWindowedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value, ElementByteSizeObserver observer)
        throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer);
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    public PaneInfo getPane() {
      return pane;
    }

    /** Returns the serialized payload that will be provided when deserializing this coder. */
    public static byte[] getPayload(ParamWindowedValueCoder<?> from) {
      // Use FullWindowedValueCoder to encode the constant members(timestamp, window, pane) in
      // ParamWindowedValueCoder
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      WindowedValue<byte[]> windowedValue =
          WindowedValue.of(EMPTY_BYTES, from.getTimestamp(), from.getWindows(), from.getPane());
      WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
          WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), from.getWindowCoder());
      try {
        windowedValueCoder.encode(windowedValue, baos);
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to encode constant members of ParamWindowedValueCoder: ", e);
      }
      return baos.toByteArray();
    }

    /** Create a {@link Coder} from its component {@link Coder coders}. */
    public static WindowedValue.ParamWindowedValueCoder<?> fromComponents(
        List<Coder<?>> components, byte[] payload) {
      Coder<? extends BoundedWindow> windowCoder =
          (Coder<? extends BoundedWindow>) components.get(1);
      WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
          WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);

      try {
        ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        WindowedValue<byte[]> windowedValue = windowedValueCoder.decode(bais);
        return WindowedValue.ParamWindowedValueCoder.of(
            components.get(0),
            windowCoder,
            windowedValue.getTimestamp(),
            windowedValue.getWindows(),
            windowedValue.getPane());
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to decode constant members from payload for ParamWindowedValueCoder: ", e);
      }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      out.writeObject(getPayload(this));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      byte[] payload = (byte[]) in.readObject();
      ParamWindowedValueCoder<?> paramWindowedValueCoder =
          fromComponents(Arrays.asList(valueCoder, getWindowCoder()), payload);
      this.timestamp = paramWindowedValueCoder.timestamp;
      this.windows = paramWindowedValueCoder.windows;
      this.pane = paramWindowedValueCoder.pane;
    }
  }
}
