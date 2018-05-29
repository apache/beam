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
package org.apache.beam.runners.core;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A customized {@link DoFnRunner} that handles late data dropping and garbage collection for
 * stateful {@link DoFn DoFns}. It registers a GC timer in {@link #processElement(WindowedValue)}
 * and does cleanup in {@link #onTimer(String, BoundedWindow, Instant, TimeDomain)}
 *
 * @param <InputT> the type of the {@link DoFn} (main) input elements
 * @param <OutputT> the type of the {@link DoFn} (main) output elements
 */
public class StatefulDoFnRunner<InputT, OutputT, W extends BoundedWindow>
    implements DoFnRunner<InputT, OutputT> {

  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "StatefulParDoDropped";

  private final DoFnRunner<InputT, OutputT> doFnRunner;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Counter droppedDueToLateness = Metrics.counter(
      StatefulDoFnRunner.class, DROPPED_DUE_TO_LATENESS_COUNTER);
  private final CleanupTimer cleanupTimer;
  private final StateCleaner stateCleaner;

  public StatefulDoFnRunner(
      DoFnRunner<InputT, OutputT> doFnRunner,
      WindowingStrategy<?, ?> windowingStrategy,
      CleanupTimer cleanupTimer,
      StateCleaner<W> stateCleaner) {
    this.doFnRunner = doFnRunner;
    this.windowingStrategy = windowingStrategy;
    this.cleanupTimer = cleanupTimer;
    this.stateCleaner = stateCleaner;
    WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();
    rejectMergingWindowFn(windowFn);
  }

  private void rejectMergingWindowFn(WindowFn<?, ?> windowFn) {
    if (!(windowFn instanceof NonMergingWindowFn)) {
      throw new UnsupportedOperationException(
          "MergingWindowFn is not supported for stateful DoFns, WindowFn is: "
              + windowFn);
    }
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return doFnRunner.getFn();
  }

  @Override
  public void startBundle() {
    doFnRunner.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InputT> input) {

    // StatefulDoFnRunner always observes windows, so we need to explode
    for (WindowedValue<InputT> value : input.explodeWindows()) {

      BoundedWindow window = value.getWindows().iterator().next();

      if (isLate(window)) {
        // The element is too late for this window.
        droppedDueToLateness.inc();
        WindowTracing.debug(
            "StatefulDoFnRunner.processElement: Dropping element at {}; window:{} "
                + "since too far behind inputWatermark:{}",
            input.getTimestamp(), window, cleanupTimer.currentInputWatermarkTime());
      } else {
        cleanupTimer.setForWindow(window);
        doFnRunner.processElement(value);
      }
    }
  }

  private boolean isLate(BoundedWindow window) {
    Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
    Instant inputWM = cleanupTimer.currentInputWatermarkTime();
    return gcTime.isBefore(inputWM);
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    if (cleanupTimer.isForWindow(timerId, window, timestamp, timeDomain)) {
      stateCleaner.clearForWindow(window);
      // There should invoke the onWindowExpiration of DoFn
    } else {
      // An event-time timer can never be late because we don't allow setting timers after GC time.
      // Ot can happen that a processing-time time fires for a late window, we need to ignore
      // this.
      if (!timeDomain.equals(TimeDomain.EVENT_TIME) && isLate(window)) {
        // don't increment the dropped counter, only do that for elements
        WindowTracing.debug(
            "StatefulDoFnRunner.onTimer: Ignoring processing-time timer at {}; window:{} "
                + "since window is too far behind inputWatermark:{}",
            timestamp, window, cleanupTimer.currentInputWatermarkTime());
      } else {
        doFnRunner.onTimer(timerId, window, timestamp, timeDomain);
      }
    }
  }

  @Override
  public void finishBundle() {
    doFnRunner.finishBundle();
  }

  /**
   * A cleaner for deciding when to clean state of window.
   *
   * <p>A runner might either (a) already know that it always has a timer set
   * for the expiration time or (b) not need a timer at all because it is
   * a batch runner that discards state when it is done.
   */
  public interface CleanupTimer {

    /**
     * Return the current, local input watermark timestamp for this computation
     * in the {@link TimeDomain#EVENT_TIME} time domain.
     */
    Instant currentInputWatermarkTime();

    /**
     * Set the garbage collect time of the window to timer.
     */
    void setForWindow(BoundedWindow window);

    /**
     * Checks whether the given timer is a cleanup timer for the window.
     */
    boolean isForWindow(
        String timerId,
        BoundedWindow window,
        Instant timestamp,
        TimeDomain timeDomain);

  }

  /**
   * A cleaner to clean all states of the window.
   */
  public interface StateCleaner<W extends BoundedWindow> {

    void clearForWindow(W window);
  }

  /**
   * A {@link StatefulDoFnRunner.CleanupTimer} implemented via {@link TimerInternals}.
   */
  public static class TimeInternalsCleanupTimer implements StatefulDoFnRunner.CleanupTimer {

    public static final String GC_TIMER_ID = "__StatefulParDoGcTimerId";

    /**
     * The amount of milliseconds by which to delay cleanup. We use this to ensure that state is
     * still available when a user timer for {@code window.maxTimestamp()} fires.
     */
    public static final long GC_DELAY_MS = 1;

    private final TimerInternals timerInternals;
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final Coder<BoundedWindow> windowCoder;

    public TimeInternalsCleanupTimer(
        TimerInternals timerInternals,
        WindowingStrategy<?, ?> windowingStrategy) {
      this.windowingStrategy = windowingStrategy;
      WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();
      windowCoder = (Coder<BoundedWindow>) windowFn.windowCoder();
      this.timerInternals = timerInternals;
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return timerInternals.currentInputWatermarkTime();
    }

    @Override
    public void setForWindow(BoundedWindow window) {
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
      // make sure this fires after any window.maxTimestamp() timers
      gcTime = gcTime.plus(GC_DELAY_MS);
      timerInternals.setTimer(StateNamespaces.window(windowCoder, window),
          GC_TIMER_ID, gcTime, TimeDomain.EVENT_TIME);
    }

    @Override
    public boolean isForWindow(
        String timerId,
        BoundedWindow window,
        Instant timestamp,
        TimeDomain timeDomain) {
      boolean isEventTimer = timeDomain.equals(TimeDomain.EVENT_TIME);
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
      gcTime = gcTime.plus(GC_DELAY_MS);
      return isEventTimer && GC_TIMER_ID.equals(timerId) && gcTime.equals(timestamp);
    }
  }

  /**
   * A {@link StatefulDoFnRunner.StateCleaner} implemented via {@link StateInternals}.
   */
  public static class StateInternalsStateCleaner<W extends BoundedWindow>
      implements StatefulDoFnRunner.StateCleaner<W> {

    private final DoFn<?, ?> fn;
    private final DoFnSignature signature;
    private final StateInternals stateInternals;
    private final Coder<W> windowCoder;

    public StateInternalsStateCleaner(
        DoFn<?, ?> fn,
        StateInternals stateInternals,
        Coder<W> windowCoder) {
      this.fn = fn;
      this.signature = DoFnSignatures.getSignature(fn.getClass());
      this.stateInternals = stateInternals;
      this.windowCoder = windowCoder;
    }

    @Override
    public void clearForWindow(W window) {
      for (Map.Entry<String, DoFnSignature.StateDeclaration> entry :
          signature.stateDeclarations().entrySet()) {
        try {
          StateSpec<?> spec = (StateSpec<?>) entry.getValue().field().get(fn);
          State state = stateInternals.state(StateNamespaces.window(windowCoder, window),
              StateTags.tagForSpec(entry.getKey(), (StateSpec) spec));
          state.clear();
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
