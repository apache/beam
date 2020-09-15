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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static java.util.Objects.requireNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Holds state of {@link Window} builder.
 *
 * @param <T> type of windowed element
 */
class WindowBuilder<T>
    implements Builders.WindowBy<WindowBuilder<T>>,
        Builders.TriggeredBy<WindowBuilder<T>>,
        Builders.AccumulationMode<WindowBuilder<T>>,
        Builders.WindowedOutput<WindowBuilder<T>> {

  private @Nullable Window<T> window;

  /**
   * Get underlying window.
   *
   * @return maybe window
   */
  Optional<Window<T>> getWindow() {
    return Optional.ofNullable(window);
  }

  /**
   * Set underlying window. This is useful for building composite operators, such as {@link
   * ReduceByKey}.
   *
   * @param window the window
   */
  void setWindow(Window<T> window) {
    checkState(this.window == null, "Window is already set.");
    this.window = window;
  }

  @Override
  public <W extends BoundedWindow> WindowBuilder<T> windowBy(WindowFn<Object, W> windowFn) {
    checkState(window == null, "Window is already set.");
    window = Window.into(windowFn);
    return this;
  }

  @Override
  public WindowBuilder<T> triggeredBy(Trigger trigger) {
    window = requireNonNull(window).triggering(trigger);
    return this;
  }

  @Override
  public WindowBuilder<T> accumulationMode(WindowingStrategy.AccumulationMode accumulationMode) {
    switch (requireNonNull(accumulationMode)) {
      case DISCARDING_FIRED_PANES:
        window = requireNonNull(window).discardingFiredPanes();
        break;
      case ACCUMULATING_FIRED_PANES:
        window = requireNonNull(window).accumulatingFiredPanes();
        break;
      default:
        throw new IllegalArgumentException("Unknown accumulation mode [" + accumulationMode + "]");
    }
    return this;
  }

  @Override
  public WindowBuilder<T> withAllowedLateness(Duration allowedLateness) {
    window = requireNonNull(window).withAllowedLateness(requireNonNull(allowedLateness));
    return this;
  }

  @Override
  public WindowBuilder<T> withAllowedLateness(
      Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
    window =
        requireNonNull(window)
            .withAllowedLateness(requireNonNull(allowedLateness), requireNonNull(closingBehavior));
    return this;
  }

  @Override
  public WindowBuilder<T> withTimestampCombiner(TimestampCombiner timestampCombiner) {
    window = requireNonNull(window).withTimestampCombiner(requireNonNull(timestampCombiner));
    return this;
  }

  @Override
  public WindowBuilder<T> withOnTimeBehavior(Window.OnTimeBehavior behavior) {
    window = requireNonNull(window).withOnTimeBehavior(requireNonNull(behavior));
    return this;
  }
}
