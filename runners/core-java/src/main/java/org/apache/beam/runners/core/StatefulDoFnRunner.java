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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.Instant;

/**
 * A customized {@link DoFnRunner} that handles late data dropping and garbage collection for
 * stateful {@link DoFn DoFns}. It registers a GC timer in {@link #processElement(WindowedValue)}
 * and does cleanup in {@link #onTimer(String, String, BoundedWindow, Instant, Instant, TimeDomain)}
 *
 * @param <InputT> the type of the {@link DoFn} (main) input elements
 * @param <OutputT> the type of the {@link DoFn} (main) output elements
 */
public class StatefulDoFnRunner<InputT, OutputT, W extends BoundedWindow>
    implements DoFnRunner<InputT, OutputT> {

  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "StatefulParDoDropped";
  private static final String SORT_BUFFER_STATE = "sortBuffer";
  private static final String SORT_BUFFER_MIN_STAMP = "sortBufferMinStamp";
  private static final String SORT_FLUSH_TIMER = "__StatefulParDoSortFlushTimerId";
  private static final String SORT_FLUSH_WATERMARK_HOLD = "flushWatermarkHold";

  private final DoFnRunner<InputT, OutputT> doFnRunner;
  private final StepContext stepContext;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Counter droppedDueToLateness =
      Metrics.counter(StatefulDoFnRunner.class, DROPPED_DUE_TO_LATENESS_COUNTER);
  private final CleanupTimer<InputT> cleanupTimer;
  private final StateCleaner stateCleaner;
  private final boolean requiresTimeSortedInput;
  private final Coder<BoundedWindow> windowCoder;
  private final StateTag<BagState<WindowedValue<InputT>>> sortBufferTag;
  private final StateTag<ValueState<Instant>> sortBufferMinStampTag =
      StateTags.makeSystemTagInternal(StateTags.value(SORT_BUFFER_MIN_STAMP, InstantCoder.of()));
  private final StateTag<WatermarkHoldState> watermarkHold =
      StateTags.watermarkStateInternal(SORT_FLUSH_WATERMARK_HOLD, TimestampCombiner.LATEST);

  public StatefulDoFnRunner(
      DoFnRunner<InputT, OutputT> doFnRunner,
      Coder<InputT> inputCoder,
      StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy,
      CleanupTimer<InputT> cleanupTimer,
      StateCleaner<W> stateCleaner,
      boolean requiresTimeSortedInput) {
    this.doFnRunner = doFnRunner;
    this.stepContext = stepContext;
    this.windowingStrategy = windowingStrategy;
    this.cleanupTimer = cleanupTimer;
    this.stateCleaner = stateCleaner;
    this.requiresTimeSortedInput = requiresTimeSortedInput;
    WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> untypedCoder = (Coder<BoundedWindow>) windowFn.windowCoder();
    this.windowCoder = untypedCoder;

    this.sortBufferTag =
        StateTags.makeSystemTagInternal(
            StateTags.bag(SORT_BUFFER_STATE, WindowedValue.getFullCoder(inputCoder, windowCoder)));

    rejectMergingWindowFn(windowFn);
  }

  private void rejectMergingWindowFn(WindowFn<?, ?> windowFn) {
    if (!(windowFn instanceof NonMergingWindowFn)) {
      throw new UnsupportedOperationException(
          "MergingWindowFn is not supported for stateful DoFns, WindowFn is: " + windowFn);
    }
  }

  public List<StateTag<?>> getSystemStateTags() {
    return Arrays.asList(sortBufferTag, sortBufferMinStampTag, watermarkHold);
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
  public void finishBundle() {
    doFnRunner.finishBundle();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    doFnRunner.onWindowExpiration(window, timestamp, key);
  }

  @Override
  public void processElement(WindowedValue<InputT> input) {

    // StatefulDoFnRunner always observes windows, so we need to explode
    for (WindowedValue<InputT> value : input.explodeWindows()) {
      BoundedWindow window = value.getWindows().iterator().next();
      if (isLate(window)) {
        // The element is too late for this window.
        reportDroppedElement(value, window);
      } else if (requiresTimeSortedInput) {
        processElementOrdered(window, value);
      } else {
        processElementUnordered(window, value);
      }
    }
  }

  private void processElementUnordered(BoundedWindow window, WindowedValue<InputT> value) {
    cleanupTimer.setForWindow(value.getValue(), window);
    doFnRunner.processElement(value);
  }

  private void processElementOrdered(BoundedWindow window, WindowedValue<InputT> value) {

    StateInternals stateInternals = stepContext.stateInternals();
    TimerInternals timerInternals = stepContext.timerInternals();

    Instant inputWatermark =
        MoreObjects.firstNonNull(
            timerInternals.currentInputWatermarkTime(), BoundedWindow.TIMESTAMP_MIN_VALUE);

    if (!inputWatermark.isAfter(
        value.getTimestamp().plus(windowingStrategy.getAllowedLateness()))) {

      StateNamespace namespace = StateNamespaces.window(windowCoder, window);
      BagState<WindowedValue<InputT>> sortBuffer = stateInternals.state(namespace, sortBufferTag);
      ValueState<Instant> minStampState = stateInternals.state(namespace, sortBufferMinStampTag);
      sortBuffer.add(value);
      Instant minStamp =
          MoreObjects.firstNonNull(minStampState.read(), BoundedWindow.TIMESTAMP_MAX_VALUE);
      if (value.getTimestamp().isBefore(minStamp)) {
        minStamp = value.getTimestamp();
        minStampState.write(minStamp);
        setupFlushTimer(namespace, window, minStamp);
      }
    } else {
      reportDroppedElement(value, window);
    }
  }

  private boolean isLate(BoundedWindow window) {
    Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
    Instant inputWM = stepContext.timerInternals().currentInputWatermarkTime();
    return gcTime.isBefore(inputWM);
  }

  private void reportDroppedElement(WindowedValue<InputT> value, BoundedWindow window) {
    droppedDueToLateness.inc();
    WindowTracing.debug(
        "StatefulDoFnRunner.processElement: Dropping element at {}; window:{} "
            + "since too far behind inputWatermark:{}",
        value.getTimestamp(),
        window,
        stepContext.timerInternals().currentInputWatermarkTime());
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    if (timerId.equals(SORT_FLUSH_TIMER)) {
      onSortFlushTimer(window, stepContext.timerInternals().currentInputWatermarkTime());
    } else if (cleanupTimer.isForWindow(timerId, window, timestamp, timeDomain)) {
      if (requiresTimeSortedInput) {
        onSortFlushTimer(window, BoundedWindow.TIMESTAMP_MAX_VALUE);
      }
      doFnRunner.onWindowExpiration(window, outputTimestamp, key);
      stateCleaner.clearForWindow(window);
    } else {
      // An event-time timer can never be late because we don't allow setting timers after GC time.
      // It can happen that a processing-time timer fires for a late window, we need to ignore
      // this.
      if (!timeDomain.equals(TimeDomain.EVENT_TIME) && isLate(window)) {
        // don't increment the dropped counter, only do that for elements
        WindowTracing.debug(
            "StatefulDoFnRunner.onTimer: Ignoring processing-time timer at {}; window:{} "
                + "since window is too far behind inputWatermark:{}",
            timestamp,
            window,
            stepContext.timerInternals().currentInputWatermarkTime());
      } else {
        doFnRunner.onTimer(
            timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
      }
    }
  }

  // this needs to be optimized (Sorted Map State)
  private void onSortFlushTimer(BoundedWindow window, Instant timestamp) {
    StateInternals stateInternals = stepContext.stateInternals();
    StateNamespace namespace = StateNamespaces.window(windowCoder, window);
    BagState<WindowedValue<InputT>> sortBuffer = stateInternals.state(namespace, sortBufferTag);
    ValueState<Instant> minStampState = stateInternals.state(namespace, sortBufferMinStampTag);
    List<WindowedValue<InputT>> keep = new ArrayList<>();
    List<WindowedValue<InputT>> flush = new ArrayList<>();
    Instant newMinStamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (WindowedValue<InputT> e : sortBuffer.read()) {
      if (!e.getTimestamp().isAfter(timestamp)) {
        flush.add(e);
      } else {
        keep.add(e);
        if (e.getTimestamp().isBefore(newMinStamp)) {
          newMinStamp = e.getTimestamp();
        }
      }
    }
    flush.stream()
        .sorted(Comparator.comparing(WindowedValue::getTimestamp))
        .forEachOrdered(e -> processElementUnordered(window, e));
    sortBuffer.clear();
    keep.forEach(sortBuffer::add);
    minStampState.write(newMinStamp);
    if (newMinStamp.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      setupFlushTimer(namespace, window, newMinStamp);
    } else {
      clearWatermarkHold(namespace);
    }
  }

  /**
   * Setup timer for flush time @{code flush}. The time is adjusted to respect allowed lateness and
   * window garbage collection time. Setup watermark hold for the flush time.
   *
   * <p>Note that this is equivalent to {@link org.apache.beam.sdk.state.Timer#withOutputTimestamp}
   * and should be reworked to use that feature once that is stable.
   */
  private void setupFlushTimer(StateNamespace namespace, BoundedWindow window, Instant flush) {
    Instant flushWithLateness = flush.plus(windowingStrategy.getAllowedLateness());
    Instant windowGcTime =
        LateDataUtils.garbageCollectionTime(window, windowingStrategy.getAllowedLateness());
    if (flushWithLateness.isAfter(windowGcTime)) {
      flushWithLateness = windowGcTime;
    }
    WatermarkHoldState watermark = stepContext.stateInternals().state(namespace, watermarkHold);
    stepContext
        .timerInternals()
        .setTimer(
            namespace,
            SORT_FLUSH_TIMER,
            SORT_FLUSH_TIMER,
            flushWithLateness,
            flush,
            TimeDomain.EVENT_TIME);
    // [BEAM-10533] check if the hold is set (pipelines before release of [BEAM-10533]
    // this can be removed in soe future versions, when we can assume there is no
    // running with this state (beam 2.23.0 and older)
    if (!watermark.isEmpty().read()) {
      watermark.clear();
    }
  }

  private void clearWatermarkHold(StateNamespace namespace) {
    stepContext.stateInternals().state(namespace, watermarkHold).clear();
  }

  /**
   * A cleaner for deciding when to clean state of window.
   *
   * <p>A runner might either (a) already know that it always has a timer set for the expiration
   * time or (b) not need a timer at all because it is a batch runner that discards state when it is
   * done.
   */
  public interface CleanupTimer<InputT> {

    /** Set the garbage collect time of the window to timer. */
    void setForWindow(InputT value, BoundedWindow window);

    /** Checks whether the given timer is a cleanup timer for the window. */
    boolean isForWindow(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain);
  }

  /** A cleaner to clean all states of the window. */
  public interface StateCleaner<W extends BoundedWindow> {

    void clearForWindow(W window);
  }

  /** A {@link StatefulDoFnRunner.CleanupTimer} implemented via {@link TimerInternals}. */
  public static class TimeInternalsCleanupTimer<InputT>
      implements StatefulDoFnRunner.CleanupTimer<InputT> {

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
        TimerInternals timerInternals, WindowingStrategy<?, ?> windowingStrategy) {
      this.windowingStrategy = windowingStrategy;
      WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();
      windowCoder = (Coder<BoundedWindow>) windowFn.windowCoder();
      this.timerInternals = timerInternals;
    }

    @Override
    public void setForWindow(InputT input, BoundedWindow window) {
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
      // make sure this fires after any window.maxTimestamp() timers
      gcTime = gcTime.plus(GC_DELAY_MS);
      timerInternals.setTimer(
          StateNamespaces.window(windowCoder, window),
          GC_TIMER_ID,
          "",
          gcTime,
          window.maxTimestamp(),
          TimeDomain.EVENT_TIME);
    }

    @Override
    public boolean isForWindow(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      boolean isEventTimer = timeDomain.equals(TimeDomain.EVENT_TIME);
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy);
      gcTime = gcTime.plus(GC_DELAY_MS);
      return isEventTimer && GC_TIMER_ID.equals(timerId) && gcTime.equals(timestamp);
    }
  }

  /** A {@link StatefulDoFnRunner.StateCleaner} implemented via {@link StateInternals}. */
  public static class StateInternalsStateCleaner<W extends BoundedWindow>
      implements StatefulDoFnRunner.StateCleaner<W> {

    private final DoFn<?, ?> fn;
    private final DoFnSignature signature;
    private final StateInternals stateInternals;
    private final Coder<W> windowCoder;

    public StateInternalsStateCleaner(
        DoFn<?, ?> fn, StateInternals stateInternals, Coder<W> windowCoder) {
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
          State state =
              stateInternals.state(
                  StateNamespaces.window(windowCoder, window),
                  StateTags.tagForSpec(entry.getKey(), (StateSpec) spec));
          state.clear();
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
