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
import org.apache.beam.runners.core.ExecutionContext.StepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateSpec;
import org.joda.time.Instant;

/**
 * A customized {@link DoFnRunner} that handles late data dropping and
 * garbage collect for stateParDo. It registers a GC timer in
 * processElement and does cleanup in onTimer.
 *
 * @param <InputT> the type of the {@link DoFn} (main) input elements
 * @param <OutputT> the type of the {@link DoFn} (main) output elements
 */
public class StatefulDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  public static final String GC_TIMER_ID = "StatefulParDoGcTimerId";
  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "StatefulParDoDropped";

  private DoFn<InputT, OutputT> fn;
  private final SimpleDoFnRunner<InputT, OutputT> doFnRunner;
  private final StepContext stepContext;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Aggregator<Long, Long> droppedDueToLateness;
  private final Coder<BoundedWindow> windowCoder;
  private final DoFnSignature signature;

  @SuppressWarnings("unchecked")
  public StatefulDoFnRunner(
      DoFn<InputT, OutputT> fn,
      SimpleDoFnRunner<InputT, OutputT> doFnRunner,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = fn;
    this.doFnRunner = doFnRunner;
    this.stepContext = stepContext;
    this.windowingStrategy = windowingStrategy;
    this.signature = DoFnSignatures.getSignature(fn.getClass());
    windowCoder = (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.droppedDueToLateness = aggregatorFactory.createAggregatorForDoFn(
        fn.getClass(), stepContext, DROPPED_DUE_TO_LATENESS_COUNTER,
        Sum.ofLongs());
  }

  @Override
  public void startBundle() {
    doFnRunner.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {

    TimerInternals timerInternals = stepContext.timerInternals();

    // StatefulDoFnRunner is always observes window
    for (WindowedValue<InputT> value : compressedElem.explodeWindows()) {

      BoundedWindow window = value.getWindows().iterator().next();
      Instant gcTime = window.maxTimestamp().plus(windowingStrategy.getAllowedLateness());

      if (!dropLateData(window)) {
        timerInternals.setTimer(StateNamespaces.window(windowCoder, window),
            GC_TIMER_ID, gcTime, TimeDomain.EVENT_TIME);
        doFnRunner.invokeProcessElement(value);
      }
    }
  }

  private boolean dropLateData(BoundedWindow window) {
    TimerInternals timerInternals = stepContext.timerInternals();
    Instant gcTime = window.maxTimestamp().plus(windowingStrategy.getAllowedLateness());
    Instant inputWM = stepContext.timerInternals().currentInputWatermarkTime();
    if (gcTime.isBefore(inputWM)) {
      // The element is too late for this window.
      droppedDueToLateness.addValue(1L);
      WindowTracing.debug(
          "StatefulDoFnRunner.processElement/onTimer: Dropping element for window:{} "
              + "since too far behind inputWatermark:{}; outputWatermark:{}",
          window, timerInternals.currentInputWatermarkTime(),
          timerInternals.currentOutputWatermarkTime());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    StateInternals<?> stateInternals = stepContext.stateInternals();

    if (GC_TIMER_ID.equals(timerId)) {
      //clean all states
      for (Map.Entry<String, DoFnSignature.StateDeclaration> entry :
          signature.stateDeclarations().entrySet()) {
        try {
          StateSpec<?, ?> spec = (StateSpec<?, ?>) entry.getValue().field().get(fn);
          State state = stateInternals.state(StateNamespaces.window(windowCoder, window),
              StateTags.tagForSpec(entry.getKey(), (StateSpec) spec));
          state.clear();
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      doFnRunner.onTimer(timerId, window, timestamp, timeDomain);
    }
  }

  @Override
  public void finishBundle() {
    doFnRunner.finishBundle();
  }

}
