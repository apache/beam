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
package org.apache.beam.sdk.transforms.windowing;

import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/**
 * Exposes window package private properties for testing.
 *
 * @param <T> type of input element
 */
public class WindowDesc<T> {

  public static <T> WindowDesc<T> of(Window<T> window) {
    return new WindowDesc<>(window);
  }

  private final Window<T> window;

  private WindowDesc(Window<T> window) {
    this.window = window;
  }

  /**
   * {@link Window#getWindowFn()}.
   *
   * @return windowFn
   */
  @SuppressWarnings("unchecked")
  public WindowFn<Object, ?> getWindowFn() {
    return (WindowFn) window.getWindowFn();
  }

  /**
   * {@link Window#getTrigger()}.
   *
   * @return trigger
   */
  public Trigger getTrigger() {
    return window.getTrigger();
  }

  /**
   * {@link Window#getAccumulationMode()}.
   *
   * @return accumulation mode
   */
  public WindowingStrategy.AccumulationMode getAccumulationMode() {
    return window.getAccumulationMode();
  }

  /**
   * {@link Window#getAllowedLateness()}.
   *
   * @return allowed lateness
   */
  public Duration getAllowedLateness() {
    return window.getAllowedLateness();
  }
}
