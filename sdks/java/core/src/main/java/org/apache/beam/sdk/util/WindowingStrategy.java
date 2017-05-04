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
package org.apache.beam.sdk.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.joda.time.Duration;

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
    ACCUMULATING_FIRED_PANES
  }

  private static final Duration DEFAULT_ALLOWED_LATENESS = Duration.ZERO;
  private static final WindowingStrategy<Object, GlobalWindow> DEFAULT = of(new GlobalWindows());

  private final WindowFn<T, W> windowFn;
  private final Trigger trigger;
  private final AccumulationMode mode;
  private final Duration allowedLateness;
  private final ClosingBehavior closingBehavior;
  private final TimestampCombiner timestampCombiner;
  private final boolean triggerSpecified;
  private final boolean modeSpecified;
  private final boolean allowedLatenessSpecified;
  private final boolean timestampCombinerSpecified;

  private WindowingStrategy(
      WindowFn<T, W> windowFn,
      Trigger trigger, boolean triggerSpecified,
      AccumulationMode mode, boolean modeSpecified,
      Duration allowedLateness, boolean allowedLatenessSpecified,
      TimestampCombiner timestampCombiner, boolean timestampCombinerSpecified,
      ClosingBehavior closingBehavior) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.triggerSpecified = triggerSpecified;
    this.mode = mode;
    this.modeSpecified = modeSpecified;
    this.allowedLateness = allowedLateness;
    this.allowedLatenessSpecified = allowedLatenessSpecified;
    this.closingBehavior = closingBehavior;
    this.timestampCombiner = timestampCombiner;
    this.timestampCombinerSpecified = timestampCombinerSpecified;
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
        DefaultTrigger.of(), false,
        AccumulationMode.DISCARDING_FIRED_PANES, false,
        DEFAULT_ALLOWED_LATENESS, false,
        TimestampCombiner.END_OF_WINDOW, false,
        ClosingBehavior.FIRE_IF_NON_EMPTY);
  }

  public WindowFn<T, W> getWindowFn() {
    return windowFn;
  }

  public Trigger getTrigger() {
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

  public TimestampCombiner getTimestampCombiner() {
    return timestampCombiner;
  }

  public boolean isTimestampCombinerSpecified() {
    return timestampCombinerSpecified;
  }

  /**
   * Returns a {@link WindowingStrategy} identical to {@code this} but with the trigger set to
   * {@code wildcardTrigger}.
   */
  public WindowingStrategy<T, W> withTrigger(Trigger trigger) {
    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, true,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        timestampCombiner, timestampCombinerSpecified,
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
        timestampCombiner, timestampCombinerSpecified,
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
        timestampCombiner, timestampCombinerSpecified,
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
        timestampCombiner, timestampCombinerSpecified,
        closingBehavior);
  }

  public WindowingStrategy<T, W> withClosingBehavior(ClosingBehavior closingBehavior) {
    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, triggerSpecified,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        timestampCombiner, timestampCombinerSpecified,
        closingBehavior);
  }

  @Experimental(Experimental.Kind.OUTPUT_TIME)
  public WindowingStrategy<T, W> withTimestampCombiner(TimestampCombiner timestampCombiner) {

    return new WindowingStrategy<T, W>(
        windowFn,
        trigger, triggerSpecified,
        mode, modeSpecified,
        allowedLateness, allowedLatenessSpecified,
        timestampCombiner, true,
        closingBehavior);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("windowFn", windowFn)
        .add("allowedLateness", allowedLateness)
        .add("trigger", trigger)
        .add("accumulationMode", mode)
        .add("timestampCombiner", timestampCombiner)
        .toString();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof WindowingStrategy)) {
      return false;
    }
    WindowingStrategy<?, ?> other = (WindowingStrategy<?, ?>) object;
    return isTriggerSpecified() == other.isTriggerSpecified()
        && isAllowedLatenessSpecified() == other.isAllowedLatenessSpecified()
        && isModeSpecified() == other.isModeSpecified()
        && isTimestampCombinerSpecified() == other.isTimestampCombinerSpecified()
        && getMode().equals(other.getMode())
        && getAllowedLateness().equals(other.getAllowedLateness())
        && getClosingBehavior().equals(other.getClosingBehavior())
        && getTrigger().equals(other.getTrigger())
        && getTimestampCombiner().equals(other.getTimestampCombiner())
        && getWindowFn().equals(other.getWindowFn());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        triggerSpecified,
        allowedLatenessSpecified,
        modeSpecified,
        timestampCombinerSpecified,
        mode,
        allowedLateness,
        closingBehavior,
        trigger,
        timestampCombiner,
        windowFn);
  }

  /**
   * Fixes all the defaults so that equals can be used to check that two strategies are the same,
   * regardless of the state of "defaulted-ness".
   */
  @VisibleForTesting
  public WindowingStrategy<T, W> fixDefaults() {
    return new WindowingStrategy<>(
        windowFn,
        trigger, true,
        mode, true,
        allowedLateness, true,
        timestampCombiner, true,
        closingBehavior);
  }
}
