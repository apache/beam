/*
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
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Class representing either a timer, or arbitrary element.
 * Used as the input type of {@link StreamingGroupAlsoByWindowsDoFn}.
 *
 * @param <E> the element type
 */
public class TimerOrElement<E> {

  /**
   * Creates a new {@code TimerOrElement<E>} representing a timer.
   *
   * @param <E> the element type
   */
  public static <E> TimerOrElement<E> timer(
      String tag, Instant timestamp, Object key) {
    return new TimerOrElement<>(tag, timestamp, key);
  }

  /**
   * Creates a new {@code TimerOrElement<E>} representing an element.
   *
   * @param <E> the element type
   */
  public static <E> TimerOrElement<E> element(E element) {
    return new TimerOrElement<>(element);
  }

  /**
   * Returns whether this is a timer or an element.
   */
  public boolean isTimer() {
    return isTimer;
  }

  /**
   * If this is a timer, returns its tag, otherwise throws an exception.
   */
  public String tag() {
    if (!isTimer) {
      throw new IllegalStateException("tag() called, but this is an element");
    }
    return tag;
  }

  /**
   * If this is a timer, returns its timestamp, otherwise throws an exception.
   */
  public Instant timestamp() {
    if (!isTimer) {
      throw new IllegalStateException("timestamp() called, but this is an element");
    }
    return timestamp;
  }

  /**
   * If this is a timer, returns its key, otherwise throws an exception.
   */
  public Object key() {
    if (!isTimer) {
      throw new IllegalStateException("key() called, but this is an element");
    }
    return key;
  }

  /**
   * If this is an element, returns it, otherwise throws an exception.
   */
  public E element() {
    if (isTimer) {
      throw new IllegalStateException("element() called, but this is a timer");
    }
    return element;
  }

  /**
   * Coder that forwards {@code ByteSizeObserver} calls to an underlying element coder.
   * {@code TimerOrElement} objects never need to be encoded, so this class does not
   * support the {@code encode} and {@code decode} methods.
   */
  @SuppressWarnings("serial")
  public static class TimerOrElementCoder<T> extends StandardCoder<TimerOrElement<T>> {
    final Coder<T> elemCoder;

    /**
     * Creates a new {@code TimerOrElement.Coder} that wraps the given {@link Coder}.
     */
    public static <T> TimerOrElementCoder<T> of(Coder<T> elemCoder) {
      return new TimerOrElementCoder<>(elemCoder);
    }

    @JsonCreator
    public static TimerOrElementCoder<?> of(
            @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
            List<Object> components) {
      return of((Coder<?>) components.get(0));
    }

    @Override
    public void encode(TimerOrElement<T> value, OutputStream outStream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimerOrElement<T> decode(InputStream inStream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(TimerOrElement<T> value, Context context) {
      if (value.isTimer()) {
        return true;
      } else {
        return elemCoder.isRegisterByteSizeObserverCheap(value.element(), context);
      }
    }

    @Override
    public void registerByteSizeObserver(
        TimerOrElement<T> value, ElementByteSizeObserver observer, Context context)
        throws Exception{
      if (!value.isTimer()) {
        elemCoder.registerByteSizeObserver(value.element(), observer, context);
      }
    }

    @Override
    public boolean isDeterministic() {
      return elemCoder.isDeterministic();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(elemCoder);
    }

    public Coder<T> getElementCoder() {
      return elemCoder;
    }

    private TimerOrElementCoder(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  private boolean isTimer;
  private String tag;
  private Instant timestamp;
  private Object key;
  private E element;

  TimerOrElement(String tag, Instant timestamp, Object key) {
    this.isTimer = true;
    this.tag = tag;
    this.timestamp = timestamp;
    this.key = key;
  }

  TimerOrElement(E element) {
    this.isTimer = false;
    this.element = element;
  }
}
