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
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

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
 * @param <V> the type of the value
 */
public abstract class WindowedValue<V> {

  protected final V value;

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp,
   * and windows.
   */
  public static <V> WindowedValue<V> of(
      V value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows) {

    if (windows.size() == 0 && BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInEmptyWindows(value);
    } else if (windows.size() == 1) {
      return of(value, timestamp, windows.iterator().next());
    } else {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp, and window.
   */
  public static <V> WindowedValue<V> of(
      V value,
      Instant timestamp,
      BoundedWindow window) {
    boolean isGlobal = GlobalWindow.INSTANCE.equals(window);
    if (isGlobal && BoundedWindow.TIMESTAMP_MIN_VALUE.equals(timestamp)) {
      return valueInGlobalWindow(value);
    } else if (isGlobal) {
      return new TimestampedValueInGlobalWindow<>(value, timestamp);
    } else {
      return new TimestampedValueInSingleWindow<>(value, timestamp, window);
    }
  }

  /**
   * Returns a {@code WindowedValue} with the given value, default timestamp,
   * and {@code GlobalWindow}.
   */
  public static <V> WindowedValue<V> valueInGlobalWindow(V value) {
    return new ValueInGlobalWindow<>(value);
  }

  /**
   * Returns a {@code WindowedValue} with the given value and default
   * timestamp and empty windows.
   */
  public static <V> WindowedValue<V> valueInEmptyWindows(V value) {
    return new ValueInEmptyWindows<>(value);
  }

  private WindowedValue(V value) {
    this.value = value;
  }

  /**
   * Returns a new {@code WindowedValue} that is a copy of this one,
   * but with a different value.
   */
  public abstract <V> WindowedValue<V> withValue(V value);

  /**
   * Returns the value of this {@code WindowedValue}.
   */
  public V getValue() {
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
  private abstract static class MinTimestampWindowedValue<V>
      extends WindowedValue<V> {
    public MinTimestampWindowedValue(V value) {
      super(value);
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
  private static class ValueInGlobalWindow<V>
      extends MinTimestampWindowedValue<V> {
    public ValueInGlobalWindow(V value) {
      super(value);
    }

    @Override
    public <V> WindowedValue<V> withValue(V value) {
      return new ValueInGlobalWindow<>(value);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ValueInGlobalWindow) {
        ValueInGlobalWindow<?> that = (ValueInGlobalWindow) o;
        return Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public String toString() {
      return "[ValueInGlobalWindow: " + value + "]";
    }
  }

  /**
   * The representation of a WindowedValue where timestamp == MIN and
   * windows == {}.
   */
  private static class ValueInEmptyWindows<V>
      extends MinTimestampWindowedValue<V> {
    public ValueInEmptyWindows(V value) {
      super(value);
    }

    @Override
    public <V> WindowedValue<V> withValue(V value) {
      return new ValueInEmptyWindows<>(value);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.emptyList();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ValueInEmptyWindows) {
        ValueInEmptyWindows<?> that = (ValueInEmptyWindows) o;
        return Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public String toString() {
      return "[ValueInEmptyWindows: " + value + "]";
    }
  }

  /**
   * The abstract superclass of WindowedValue representations where
   * timestamp is arbitrary.
   */
  private abstract static class TimestampedWindowedValue<V>
      extends WindowedValue<V> {
    protected final Instant timestamp;

    public TimestampedWindowedValue(V value,
                                    Instant timestamp) {
      super(value);
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
  private static class TimestampedValueInGlobalWindow<V>
      extends TimestampedWindowedValue<V> {
    public TimestampedValueInGlobalWindow(V value,
                                          Instant timestamp) {
      super(value, timestamp);
    }

    @Override
    public <V> WindowedValue<V> withValue(V value) {
      return new TimestampedValueInGlobalWindow<>(value, timestamp);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInGlobalWindow) {
        TimestampedValueInGlobalWindow<?> that =
            (TimestampedValueInGlobalWindow) o;
        return this.timestamp.getMillis() == that.timestamp.getMillis()
            && Objects.equals(that.value, this.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value) ^ ((int) timestamp.getMillis());
    }

    @Override
    public String toString() {
      return "[ValueInGlobalWindow: " + value
          + ", timestamp: " + timestamp.getMillis() + "]";
    }
  }

  /**
   * The representation of a WindowedValue where timestamp is arbitrary and
   * windows == a single non-Global window.
   */
  private static class TimestampedValueInSingleWindow<V>
      extends TimestampedWindowedValue<V> {
    private final BoundedWindow window;

    public TimestampedValueInSingleWindow(V value,
                                          Instant timestamp,
                                          BoundedWindow window) {
      super(value, timestamp);
      this.window = checkNotNull(window);
    }

    @Override
    public <V> WindowedValue<V> withValue(V value) {
      return new TimestampedValueInSingleWindow<>(value, timestamp, window);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.singletonList(window);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInSingleWindow) {
        TimestampedValueInSingleWindow<?> that =
            (TimestampedValueInSingleWindow) o;
        return Objects.equals(that.value, this.value)
            && that.timestamp.isEqual(this.timestamp)
            && that.window.equals(this.window);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, timestamp, window);
    }

    @Override
    public String toString() {
      return "[WindowedValue: " + value
          + ", timestamp: " + timestamp.getMillis()
          + ", window: " + window + "]";
    }
  }

  /**
   * The representation of a WindowedValue, excluding the special
   * cases captured above.
   */
  private static class TimestampedValueInMultipleWindows<V>
      extends TimestampedWindowedValue<V> {
    private Collection<? extends BoundedWindow> windows;

    public TimestampedValueInMultipleWindows(
        V value,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
      super(value, timestamp);
      this.windows = checkNotNull(windows);
    }

    @Override
    public <V> WindowedValue<V> withValue(V value) {
      return new TimestampedValueInMultipleWindows<>(value, timestamp, windows);
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimestampedValueInMultipleWindows) {
        TimestampedValueInMultipleWindows<?> that =
            (TimestampedValueInMultipleWindows) o;
        if (Objects.equals(that.value, this.value)
            && that.timestamp.isEqual(this.timestamp)) {
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
      return Objects.hash(value, timestamp, windows);
    }

    @Override
    public String toString() {
      return "[WindowedValue: " + value
          + ", timestamp: " + timestamp.getMillis()
          + ", windows: " + windows + "]";
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
    private static final long serialVersionUID = 0;

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
    public abstract <V> WindowedValueCoder<V> withValueCoder(Coder<V> valueCoder);
  }

  /**
   * Coder for {@code WindowedValue}.
   */
  public static class FullWindowedValueCoder<T> extends WindowedValueCoder<T> {
    private static final long serialVersionUID = 0;

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

    @SuppressWarnings("unchecked")
    FullWindowedValueCoder(Coder<T> valueCoder,
                           Coder<? extends BoundedWindow> windowCoder) {
      super(valueCoder);
      this.windowCoder = checkNotNull(windowCoder);
      // It's not possible to statically type-check correct use of the
      // windowCoder (we have to ensure externally that we only get
      // windows of the class handled by windowCoder), so type
      // windowsCoder in a way that makes encode() and decode() work
      // right, and cast the window type away here.
      this.windowsCoder = (Coder) CollectionCoder.of(this.windowCoder);
    }

    public Coder<? extends BoundedWindow> getWindowCoder() {
      return windowCoder;
    }

    public Coder<Collection<? extends BoundedWindow>> getWindowsCoder() {
      return windowsCoder;
    }

    @Override
    public <V> WindowedValueCoder<V> withValueCoder(Coder<V> valueCoder) {
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
    }

    @Override
    public WindowedValue<T> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Context nestedContext = context.nested();
      T value = valueCoder.decode(inStream, nestedContext);
      Instant timestamp = InstantCoder.of().decode(inStream, nestedContext);
      Collection<? extends BoundedWindow> windows =
          windowsCoder.decode(inStream, nestedContext);
      return WindowedValue.of(value, timestamp, windows);
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
    private static final long serialVersionUID = 0;

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
    public <V> WindowedValueCoder<V> withValueCoder(Coder<V> valueCoder) {
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
