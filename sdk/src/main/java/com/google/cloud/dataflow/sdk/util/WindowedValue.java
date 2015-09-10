/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.util.Structs.addBoolean;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CollectionCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable triple of value, timestamp, and windows.
 *
 * @param <T> the type of the value
 */
public abstract class WindowedValue<T> {

  protected final T value;
  protected final PaneInfo pane;

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp,
   * and windows.
   */
  public static <T> WindowedValue<T> of(
      T value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane) {
    Preconditions.checkNotNull(pane);

    if (windows.size() == 0 && BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInEmptyWindows(value, pane);
    } else if (windows.size() == 1) {
      return of(value, timestamp, windows.iterator().next(), pane);
    } else {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows, pane);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp, and window.
   */
  public static <T> WindowedValue<T> of(
      T value,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {
    Preconditions.checkNotNull(pane);

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
   * Returns a {@code WindowedValue} with the given value and timestamp,
   * {@code GlobalWindow} and default pane.
   */
  public static <T> WindowedValue<T> timestampedValueInGlobalWindow(T value, Instant timestamp) {
    if (BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInGlobalWindow(value);
    } else {
      return new TimestampedValueInGlobalWindow<>(value, timestamp, PaneInfo.NO_FIRING);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value in no windows, and the default timestamp
   * and pane.
   */
  public static <T> WindowedValue<T> valueInEmptyWindows(T value) {
    return new ValueInEmptyWindows<T>(value, PaneInfo.NO_FIRING);
  }

  /**
   * Returns a {@code WindowedValue} with the given value in no windows, and the default timestamp
   * and the specified pane.
   */
  public static <T> WindowedValue<T> valueInEmptyWindows(T value, PaneInfo pane) {
    return new ValueInEmptyWindows<T>(value, pane);
  }

  private WindowedValue(T value, PaneInfo pane) {
    this.value = value;
    this.pane = checkNotNull(pane);
  }

  /**
   * Returns a new {@code WindowedValue} that is a copy of this one, but with a different value,
   * which may have a new type {@code NewT}.
   */
  public abstract <NewT> WindowedValue<NewT> withValue(NewT value);

  /**
   * Returns the value of this {@code WindowedValue}.
   */
  public T getValue() {
    return value;
  }

  /**
   * Returns the timestamp of this {@code WindowedValue}.
   */
  public abstract Instant getTimestamp();

  /**
   * Returns the windows of this {@code WindowedValue}.
   */
  public abstract Collection<? extends BoundedWindow> getWindows();

  /**
   * Returns the pane of this {@code WindowedValue} in its window.
   */
  public PaneInfo getPane() {
    return pane;
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  @Override
  public abstract String toString();

  private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  /**
   * The abstract superclass of WindowedValue representations where
   * timestamp == MIN.
   */
  private abstract static class MinTimestampWindowedValue<T>
      extends WindowedValue<T> {
    public MinTimestampWindowedValue(T value, PaneInfo pane) {
      super(value, pane);
    }

    @Override
    public Instant getTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }

  /**
   * The representation of a WindowedValue where timestamp == MIN and
   * windows == {GlobalWindow}.
   */
  private static class ValueInGlobalWindow<T>
      extends MinTimestampWindowedValue<T> {
    public ValueInGlobalWindow(T value, PaneInfo pane) {
      super(value, pane);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new ValueInGlobalWindow<>(value, pane);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ValueInGlobalWindow) {
        ValueInGlobalWindow<?> that = (ValueInGlobalWindow<?>) o;
        return Objects.equals(that.pane, this.pane)
            && Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, pane);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .add("pane", pane)
          .toString();
    }
  }

  /**
   * The representation of a WindowedValue where timestamp == MIN and
   * windows == {}.
   */
  private static class ValueInEmptyWindows<T>
      extends MinTimestampWindowedValue<T> {
    public ValueInEmptyWindows(T value, PaneInfo pane) {
      super(value, pane);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new ValueInEmptyWindows<>(value, pane);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.emptyList();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ValueInEmptyWindows) {
        ValueInEmptyWindows<?> that = (ValueInEmptyWindows<?>) o;
        return Objects.equals(that.pane, this.pane)
            && Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, pane);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .add("pane", pane)
          .toString();
    }
  }

  /**
   * The abstract superclass of WindowedValue representations where
   * timestamp is arbitrary.
   */
  private abstract static class TimestampedWindowedValue<T>
      extends WindowedValue<T> {
    protected final Instant timestamp;

    public TimestampedWindowedValue(T value,
                                    Instant timestamp,
                                    PaneInfo pane) {
      super(value, pane);
      this.timestamp = checkNotNull(timestamp);
    }

    @Override
    public Instant getTimestamp() {
      return timestamp;
    }
  }

  /**
   * The representation of a WindowedValue where timestamp {@code >}
   * MIN and windows == {GlobalWindow}.
   */
  private static class TimestampedValueInGlobalWindow<T>
      extends TimestampedWindowedValue<T> {
    public TimestampedValueInGlobalWindow(T value,
                                          Instant timestamp,
                                          PaneInfo pane) {
      super(value, timestamp, pane);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new TimestampedValueInGlobalWindow<>(value, timestamp, pane);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInGlobalWindow) {
        TimestampedValueInGlobalWindow<?> that =
            (TimestampedValueInGlobalWindow<?>) o;
        return this.timestamp.isEqual(that.timestamp) // don't compare chronology objects
            && Objects.equals(that.pane, this.pane)
            && Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, pane, timestamp.getMillis());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .add("timestamp", timestamp)
          .add("pane", pane)
          .toString();
    }
  }

  /**
   * The representation of a WindowedValue where timestamp is arbitrary and
   * windows == a single non-Global window.
   */
  private static class TimestampedValueInSingleWindow<T>
      extends TimestampedWindowedValue<T> {
    private final BoundedWindow window;

    public TimestampedValueInSingleWindow(T value,
                                          Instant timestamp,
                                          BoundedWindow window,
                                          PaneInfo pane) {
      super(value, timestamp, pane);
      this.window = checkNotNull(window);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new TimestampedValueInSingleWindow<>(value, timestamp, window, pane);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.singletonList(window);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInSingleWindow) {
        TimestampedValueInSingleWindow<?> that =
            (TimestampedValueInSingleWindow<?>) o;
        return Objects.equals(that.value, this.value)
            && this.timestamp.isEqual(that.timestamp) // don't compare chronology objects
            && Objects.equals(that.pane, this.pane)
            && Objects.equals(that.window, this.window);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, timestamp.getMillis(), pane, window);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .add("timestamp", timestamp)
          .add("window", window)
          .add("pane", pane)
          .toString();
    }
  }

  /**
   * The representation of a WindowedValue, excluding the special
   * cases captured above.
   */
  private static class TimestampedValueInMultipleWindows<T>
      extends TimestampedWindowedValue<T> {
    private Collection<? extends BoundedWindow> windows;

    public TimestampedValueInMultipleWindows(
        T value,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      super(value, timestamp, pane);
      this.windows = checkNotNull(windows);
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows, pane);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInMultipleWindows) {
        TimestampedValueInMultipleWindows<?> that =
            (TimestampedValueInMultipleWindows<?>) o;
        if (this.timestamp.isEqual(that.timestamp) // don't compare chronology objects
            && Objects.equals(that.value, this.value)
            && Objects.equals(that.pane, this.pane)) {
          ensureWindowsAreASet();
          that.ensureWindowsAreASet();
          return that.windows.equals(this.windows);
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      ensureWindowsAreASet();
      return Objects.hash(value, timestamp.getMillis(), pane, windows);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .add("timestamp", timestamp)
          .add("windows", windows)
          .add("pane", pane)
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
   * Returns the {@code Coder} to use for a {@code WindowedValue<T>},
   * using the given valueCoder and windowCoder.
   */
  public static <T> FullWindowedValueCoder<T> getFullCoder(
      Coder<T> valueCoder,
      Coder<? extends BoundedWindow> windowCoder) {
    return FullWindowedValueCoder.of(valueCoder, windowCoder);
  }

  /**
   * Returns the {@code ValueOnlyCoder} from the given valueCoder.
   */
  public static <T> ValueOnlyWindowedValueCoder<T> getValueOnlyCoder(Coder<T> valueCoder) {
    return ValueOnlyWindowedValueCoder.of(valueCoder);
  }

  /**
   * Abstract class for {@code WindowedValue} coder.
   */
  public abstract static class WindowedValueCoder<T>
      extends StandardCoder<WindowedValue<T>> {
    final Coder<T> valueCoder;

    WindowedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    /**
     * Returns the value coder.
     */
    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    /**
     * Returns a new {@code WindowedValueCoder} that is a copy of this one,
     * but with a different value coder.
     */
    public abstract <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder);
  }

  /**
   * Coder for {@code WindowedValue}.
   */
  public static class FullWindowedValueCoder<T> extends WindowedValueCoder<T> {
    private final Coder<? extends BoundedWindow> windowCoder;
    // Precompute and cache the coder for a list of windows.
    private final Coder<Collection<? extends BoundedWindow>> windowsCoder;

    public static <T> FullWindowedValueCoder<T> of(
        Coder<T> valueCoder,
        Coder<? extends BoundedWindow> windowCoder) {
      return new FullWindowedValueCoder<>(valueCoder, windowCoder);
    }

    @JsonCreator
    public static FullWindowedValueCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      checkArgument(components.size() == 2,
                    "Expecting 2 components, got " + components.size());
      @SuppressWarnings("unchecked")
      Coder<? extends BoundedWindow> window = (Coder<? extends BoundedWindow>) components.get(1);
      return of(components.get(0), window);
    }

    FullWindowedValueCoder(Coder<T> valueCoder,
                           Coder<? extends BoundedWindow> windowCoder) {
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
    public void encode(WindowedValue<T> windowedElem,
                       OutputStream outStream,
                       Context context)
        throws CoderException, IOException {
      Context nestedContext = context.nested();
      valueCoder.encode(windowedElem.getValue(), outStream, nestedContext);
      InstantCoder.of().encode(
          windowedElem.getTimestamp(), outStream, nestedContext);
      windowsCoder.encode(windowedElem.getWindows(), outStream, nestedContext);
      PaneInfoCoder.INSTANCE.encode(windowedElem.getPane(), outStream, context);
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Context nestedContext = context.nested();
      T value = valueCoder.decode(inStream, nestedContext);
      Instant timestamp = InstantCoder.of().decode(inStream, nestedContext);
      Collection<? extends BoundedWindow> windows =
          windowsCoder.decode(inStream, nestedContext);
      PaneInfo pane = PaneInfoCoder.INSTANCE.decode(inStream, nestedContext);
      return WindowedValue.of(value, timestamp, windows, pane);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          "FullWindowedValueCoder requires a deterministic valueCoder",
          valueCoder);
      verifyDeterministic(
          "FullWindowedValueCoder requires a deterministic windowCoder",
          windowCoder);
    }

    @Override
    public void registerByteSizeObserver(WindowedValue<T> value,
                                         ElementByteSizeObserver observer,
                                         Context context) throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer, context);
      InstantCoder.of().registerByteSizeObserver(value.getTimestamp(), observer, context);
      windowsCoder.registerByteSizeObserver(value.getWindows(), observer, context);
    }

    @Override
    public CloudObject asCloudObject() {
      CloudObject result = super.asCloudObject();
      addBoolean(result, PropertyNames.IS_WRAPPER, true);
      return result;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Arrays.<Coder<?>>asList(valueCoder, windowCoder);
    }
  }

  /**
   * Coder for {@code WindowedValue}.
   *
   * <p>A {@code ValueOnlyWindowedValueCoder} only encodes and decodes the value. It drops
   * timestamp and windows for encoding, and uses defaults timestamp, and windows for decoding.
   */
  public static class ValueOnlyWindowedValueCoder<T> extends WindowedValueCoder<T> {
    public static <T> ValueOnlyWindowedValueCoder<T> of(
        Coder<T> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    @JsonCreator
    public static ValueOnlyWindowedValueCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      checkArgument(components.size() == 1, "Expecting 1 component, got " + components.size());
      return of(components.get(0));
    }

    ValueOnlyWindowedValueCoder(Coder<T> valueCoder) {
      super(valueCoder);
    }

    @Override
    public <NewT> WindowedValueCoder<NewT> withValueCoder(Coder<NewT> valueCoder) {
      return new ValueOnlyWindowedValueCoder<>(valueCoder);
    }

    @Override
    public void encode(WindowedValue<T> windowedElem, OutputStream outStream, Context context)
        throws CoderException, IOException {
      valueCoder.encode(windowedElem.getValue(), outStream, context);
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
          "ValueOnlyWindowedValueCoder requires a deterministic valueCoder",
          valueCoder);
    }

    @Override
    public void registerByteSizeObserver(
        WindowedValue<T> value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      valueCoder.registerByteSizeObserver(value.getValue(), observer, context);
    }

    @Override
    public CloudObject asCloudObject() {
      CloudObject result = super.asCloudObject();
      addBoolean(result, PropertyNames.IS_WRAPPER, true);
      return result;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.<Coder<?>>asList(valueCoder);
    }
  }
}
