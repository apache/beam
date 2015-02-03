/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * An immutable triple of value, timestamp, and windows.
 *
 * @param <V> the type of the value
 */
public class WindowedValue<V> {

  private final V value;
  private final Instant timestamp;
  private final Collection<? extends BoundedWindow> windows;

  /**
   * Returns a {@code WindowedValue} with the given value, timestamp, and windows.
   */
  public static <V> WindowedValue<V> of(
      V value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows) {
    return new WindowedValue<>(value, timestamp, windows);
  }

  /**
   * Returns a {@code WindowedValue} with the given value, default timestamp,
   * and {@code GlobalWindow}.
   */
  public static <V> WindowedValue<V> valueInGlobalWindow(V value) {
    return new WindowedValue<>(value,
                               new Instant(Long.MIN_VALUE),
                               Arrays.asList(GlobalWindow.INSTANCE));
  }

  /**
   * Returns a {@code WindowedValue} with the given value and default timestamp and empty windows.
   */
  public static <V> WindowedValue<V> valueInEmptyWindows(V value) {
    return new WindowedValue<V>(value,
                                new Instant(Long.MIN_VALUE),
                                Collections.<BoundedWindow>emptyList());
  }

  private WindowedValue(V value,
                        Instant timestamp,
                        Collection<? extends BoundedWindow> windows) {
    checkNotNull(timestamp);
    checkNotNull(windows);

    this.value = value;
    this.timestamp = timestamp;
    this.windows = windows;
  }

  /**
   * Returns a new {@code WindowedValue} that is a copy of this one, but with a different value.
   */
  public <V> WindowedValue<V> withValue(V value) {
    return new WindowedValue<>(value, this.timestamp, this.windows);
  }

  /**
   * Returns the value of this {@code WindowedValue}.
   */
  public V getValue() {
    return value;
  }

  /**
   * Returns the timestamp of this {@code WindowedValue}.
   */
  public Instant getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the windows of this {@code WindowedValue}.
   */
  public Collection<? extends BoundedWindow> getWindows() {
    return windows;
  }

  /**
   * Returns the {@code Coder} to use for a {@code WindowedValue<T>},
   * using the given valueCoder and windowCoder.
   */
  public static <T> WindowedValueCoder<T> getFullCoder(
      Coder<T> valueCoder,
      Coder<? extends BoundedWindow> windowCoder) {
    return FullWindowedValueCoder.of(valueCoder, windowCoder);
  }

  /**
   * Returns the {@code ValueOnlyCoder} from the given valueCoder.
   */
  public static <T> WindowedValueCoder<T> getValueOnlyCoder(Coder<T> valueCoder) {
    return ValueOnlyWindowedValueCoder.of(valueCoder);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WindowedValue) {
      WindowedValue<?> that = (WindowedValue) o;
      if (Objects.equals(that.value, this.value)
          && that.timestamp.isEqual(timestamp)
          && that.windows.size() == windows.size()) {
        for (Iterator<?> thatIterator = that.windows.iterator(), thisIterator = windows.iterator();
            thatIterator.hasNext() && thisIterator.hasNext();
            /* do nothing */) {
          if (!thatIterator.next().equals(thisIterator.next())) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, timestamp, Arrays.hashCode(windows.toArray()));
  }

  @Override
  public String toString() {
    return "[WindowedValue: " + value + ", timestamp: " + timestamp.getMillis()
        + ", windows: " + windows + "]";
  }

  /////////////////////////////////////////////////////////////////////////////

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
    public boolean isDeterministic() {
      return valueCoder.isDeterministic() && windowCoder.isDeterministic();
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
   * <P>A {@code ValueOnlyWindowedValueCoder} only encodes and decodes the value. It drops
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
    public boolean isDeterministic() {
      return valueCoder.isDeterministic();
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
