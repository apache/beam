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
package org.apache.beam.runners.dataflow.worker.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A value in no windows.
 *
 * <p>Uses of this class can probably be replaced by alternate design patterns, and probably should
 * be.
 *
 * <p>Technically, such a value does not exist and can be dropped by a runner at any time. However,
 * the Dataflow worker uses this in places to pass a {@link WindowedValue} to a method that requires
 * one without having to provide the global window, which could alter size estimations, etc.
 */
public class ValueInEmptyWindows<T> extends WindowedValue<T> {
  private final T value;

  public ValueInEmptyWindows(T value) {
    this.value = value;
  }

  @Override
  public PaneInfo getPane() {
    return PaneInfo.NO_FIRING;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public <NewT> WindowedValue<NewT> withValue(NewT newValue) {
    return new ValueInEmptyWindows<>(newValue);
  }

  @Override
  public Instant getTimestamp() {
    return BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  @Override
  public Collection<? extends BoundedWindow> getWindows() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o instanceof ValueInEmptyWindows) {
      ValueInEmptyWindows<?> that = (ValueInEmptyWindows<?>) o;
      return Objects.equals(this.getValue(), that.getValue());
    } else {
      return super.equals(o);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getValue());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("value", getValue())
        .add("pane", getPane())
        .toString();
  }
}
