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
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import org.joda.time.Duration;

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

  /**
   * The accumulation modes that can be used with windowing.
   */
  public enum AccumulationMode {
    DISCARDING_FIRED_PANES,
    ACCUMULATING_FIRED_PANES;
  }

  private static final Duration DEFAULT_ALLOWED_LATENESS = Duration.ZERO;
  private static final WindowingStrategy<Object, GlobalWindow> DEFAULT = of(new GlobalWindows());

  private static final long serialVersionUID = 0L;

  private final WindowFn<T, W> windowFn;
  private final ExecutableTrigger<W> trigger;
  private final AccumulationMode mode;
  private final Duration allowedLateness;
  private final ClosingBehavior closingBehavior;
  private final boolean triggerSpecified;
  private final boolean modeSpecified;
  private final boolean allowedLatenessSpecified;

  private WindowingStrategy(
      WindowFn<T, W> windowFn,
      ExecutableTrigger<W> trigger, boolean triggerSpecified,
      AccumulationMode mode, boolean modeSpecified,
      Duration allowedLateness, boolean allowedLatenessSpecified,
      ClosingBehavior closingBehavior) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.triggerSpecified = triggerSpecified;
    this.mode = mode;
    this.modeSpecified = modeSpecified;
    this.allowedLateness = allowedLateness;
    this.allowedLatenessSpecified = allowedLatenessSpecified;
    this.closingBehavior = closingBehavior;
  }

  /**
   * Return a fully specified, default windowing strategy.
   */
  public static WindowingStrategy<Object, GlobalWindow> globalDefault() {
    return DEFAULT;
  }

  public static <T, W extends BoundedWindow> WindowingStrategy<T, W> of(
      WindowFn<T, W> windowFn) {
    return new WindowingStrategy<>(windowFn,
        ExecutableTrigger.create(DefaultTrigger.<W>of()), false,
        AccumulationMode.DISCARDING_FIRED_PANES, false,
        DEFAULT_ALLOWED_LATENESS, false,
        ClosingBehavior.FIRE_IF_NON_EMPTY);
  }

  public WindowFn<T, W> getWindowFn() {
    return windowFn;
  }

  public ExecutableTrigger<W> getTrigger() {
    return trigger;
  }

  public boolean isTriggerSpecified() {
    return triggerSpecified;
  }

  public Duration getAllowedLateness() {
    return allowedLateness;
  }

  public boolean isAllowedLatenessSpecified() {
    return allowedLatenessSpecified;
  }

  public AccumulationMode getMode() {
    return mode;
  }

  public boolean isModeSpecified() {
    return modeSpecified;
  }

  public ClosingBehavior getClosingBehavior() {
    return closingBehavior;
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the trigger set to
   * {@code wildcardTrigger}.
   */
  public WindowingStrategy<T, W> withTrigger(Trigger<?> wildcardTrigger) {
    @SuppressWarnings("unchecked")
    Trigger<W> typedTrigger = (Trigger<W>) wildcardTrigger;
    return new WindowingStrategy<T, W>(
        windowFn,
        ExecutableTrigger.create(typedTrigger), true,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        closingBehavior);
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the accumulation mode
   * set to {@code mode}.
   */
  public WindowingStrategy<T, W> withMode(AccumulationMode mode) {
    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, triggerSpecified,
        mode, true,
        allowedLateness, allowedLatenessSpecified,
        closingBehavior);
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the window function
   * set to {@code wildcardWindowFn}.
   */
  public WindowingStrategy<T, W> withWindowFn(WindowFn<?, ?> wildcardWindowFn) {
    @SuppressWarnings("unchecked")
    WindowFn<T, W> typedWindowFn = (WindowFn<T, W>) wildcardWindowFn;
    return new WindowingStrategy<T, W>(
        typedWindowFn,
        trigger, triggerSpecified,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        closingBehavior);
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the allowed lateness
   * set to {@code allowedLateness}.
   */
  public WindowingStrategy<T, W> withAllowedLateness(Duration allowedLateness) {
    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, triggerSpecified,
        mode, modeSpecified,
        allowedLateness, true,
        closingBehavior);
  }

  public WindowingStrategy<T, W> withClosingBehavior(ClosingBehavior closingBehavior) {
    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, triggerSpecified,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        closingBehavior);
  }

  @Override
  public String toString() {
    return String.format("%s, %s, %s",
        StringUtils.approximateSimpleName(windowFn.getClass()),
        trigger.toString(),
        mode.toString());
  }
}
