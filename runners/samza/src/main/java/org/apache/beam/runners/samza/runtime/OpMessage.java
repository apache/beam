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
package org.apache.beam.runners.samza.runtime;

import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Actual message type used in Samza {@link org.apache.samza.application.StreamApplication}. It
 * contains either an element of main inputs or the collection results from a view (used as side
 * input).
 */
public class OpMessage<T> {
  /**
   * Type of the element(s) in the message.
   *
   * <ul>
   *   <li>ELEMENT - an element from main inputs.
   *   <li>SIDE_INPUT - a collection of elements from a view.
   * </ul>
   */
  public enum Type {
    ELEMENT,
    SIDE_INPUT,
    SIDE_INPUT_WATERMARK
  }

  private final Type type;
  private final WindowedValue<T> element;
  private final String viewId;
  private final WindowedValue<? extends Iterable<?>> viewElements;
  private final Instant sideInputWatermark;

  public static <T> OpMessage<T> ofElement(WindowedValue<T> element) {
    return new OpMessage<>(Type.ELEMENT, element, null, null, null);
  }

  public static <T, ElemT> OpMessage<T> ofSideInput(
      String viewId, WindowedValue<? extends Iterable<ElemT>> elements) {
    return new OpMessage<>(Type.SIDE_INPUT, null, viewId, elements, null);
  }

  public static <T, ElemT> OpMessage<T> ofSideInputWatermark(Instant watermark) {
    return new OpMessage<>(Type.SIDE_INPUT_WATERMARK, null, null, null, watermark);
  }

  private OpMessage(
      Type type,
      WindowedValue<T> element,
      String viewId,
      WindowedValue<? extends Iterable<?>> viewElements,
      Instant sideInputWatermark) {
    this.type = type;
    this.element = element;
    this.viewId = viewId;
    this.viewElements = viewElements;
    this.sideInputWatermark = sideInputWatermark;
  }

  public Type getType() {
    return type;
  }

  public WindowedValue<T> getElement() {
    ensureType(Type.ELEMENT, "getElement");
    return element;
  }

  public String getViewId() {
    ensureType(Type.SIDE_INPUT, "getViewId");
    return viewId;
  }

  public WindowedValue<? extends Iterable<?>> getViewElements() {
    ensureType(Type.SIDE_INPUT, "getViewElements");
    return viewElements;
  }

  public Instant getSideInputWatermark() {
    return sideInputWatermark;
  }

  private void ensureType(Type type, String method) {
    if (this.type != type) {
      throw new IllegalStateException(
          String.format("Calling %s requires type %s, but was type %s", method, type, this.type));
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OpMessage<?> opMessage = (OpMessage<?>) o;

    if (type != opMessage.type) {
      return false;
    }

    if (element != null ? !element.equals(opMessage.element) : opMessage.element != null) {
      return false;
    }

    if (viewId != null ? !viewId.equals(opMessage.viewId) : opMessage.viewId != null) {
      return false;
    }

    return viewElements != null
        ? viewElements.equals(opMessage.viewElements)
        : opMessage.viewElements == null;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (element != null ? element.hashCode() : 0);
    result = 31 * result + (viewId != null ? viewId.hashCode() : 0);
    result = 31 * result + (viewElements != null ? viewElements.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OpMessage{"
        + "type="
        + type
        + ", element="
        + element
        + ", viewId='"
        + viewId
        + '\''
        + ", viewElements="
        + viewElements
        + '}';
  }
}
