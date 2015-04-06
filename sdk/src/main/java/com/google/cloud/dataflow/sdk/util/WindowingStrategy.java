/*
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
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import java.io.Serializable;

/**
 * A {@code WindowingStrategy} describes the windowing behavior for a specific collection of values.
 * It has both a {@link WindowFn} describing how elements are assigned to windows and a
 * {@link Trigger} that controls when output is produced for each window.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the
 *            windows used by this {@code WindowingStrategy}
 */
public class WindowingStrategy<T, W extends BoundedWindow> implements Serializable {

  private static final WindowingStrategy<Object, GlobalWindow> DEFAULT = of(new GlobalWindows());

  private static final long serialVersionUID = 0L;

  private final WindowFn<T, W> windowFn;
  private final Trigger<W> trigger;

  private WindowingStrategy(WindowFn<T, W> windowFn, Trigger<W> trigger) {
    this.windowFn = windowFn;
    this.trigger = trigger;
  }

  public static WindowingStrategy<Object, GlobalWindow> globalDefault() {
    return DEFAULT;
  }

  /**
   * Create a {@code WindowingStrategy} for the given {@code windowFn}, using the
   * {@link DefaultTrigger}.
   */
  public static <T, W extends BoundedWindow> WindowingStrategy<T, W> of(WindowFn<T, W> windowFn) {
    return of(windowFn, DefaultTrigger.<W>of());
  }

  /**
   * Create a {@code WindowingStrategy} for the given {@code windowFn} and {@code trigger}.
   */
  public static <T, W extends BoundedWindow> WindowingStrategy<T, W> of(
      WindowFn<T, W> windowFn, Trigger<W> trigger) {
    return new WindowingStrategy<>(windowFn, trigger);
  }

  public WindowFn<T, W> getWindowFn() {
    return windowFn;
  }

  public Trigger<W> getTrigger() {
    return trigger;
  }
}
