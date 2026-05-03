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
package org.apache.beam.runners.dataflow.worker;

import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.WindowedValue;
import org.joda.time.Instant;

/**
 * Runs a DoFn by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the DoFn's (main) input elements
 * @param <OutputT> the type of the DoFn's (main) output elements
 * @param <W> the type of the windows of the main input
 */
public class StreamingSideInputDoFnRunner<InputT, OutputT, W extends BoundedWindow>
    implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> simpleDoFnRunner;
  private final StreamingSideInputFetcher<InputT, W> sideInputFetcher;
  private final Coder<W> windowCoder;

  public StreamingSideInputDoFnRunner(
      DoFnRunner<InputT, OutputT> simpleDoFnRunner,
      StreamingSideInputFetcher<InputT, W> sideInputFetcher,
      Coder<W> windowCoder) {
    this.simpleDoFnRunner = simpleDoFnRunner;
    this.sideInputFetcher = sideInputFetcher;
    this.windowCoder = windowCoder;
  }

  @Override
  public void startBundle() {
    simpleDoFnRunner.startBundle();
    sideInputFetcher.prefetchBlockedMap();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<TimerInternals.TimerData>> timerBags =
        sideInputFetcher.prefetchTimers(readyWindows, false);
    Iterable<BagState<WindowedValue<InputT>>> elementsBags =
        sideInputFetcher.prefetchElements(readyWindows);
    Iterable<ValueState<StreamingSideInputFetcher.WindowExpirationArguments>> windowExpirationValues =
      sideInputFetcher.prefetchWindowExpirations(readyWindows);

    // Run the DoFn code now that all side inputs are ready.
    for (BagState<TimerInternals.TimerData> timerBag : timerBags) {
      sideInputFetcher.timersUnblocked(timerBag.read());
      timerBag.clear();
    }

    for (BagState<WindowedValue<InputT>> elementsBag : elementsBags) {
      Iterable<WindowedValue<InputT>> elements = elementsBag.read();
      for (WindowedValue<InputT> elem : elements) {
        simpleDoFnRunner.processElement(elem);
      }
      elementsBag.clear();
    }

    for (ValueState<StreamingSideInputFetcher.WindowExpirationArguments> windowExpiration : windowExpirationValues) {
      StreamingSideInputFetcher.WindowExpirationArguments args = windowExpiration.read();
      simpleDoFnRunner.onWindowExpiration(window, args.getTimestamp(), args.getKey());
      windowExpiration.clear();
    }
    sideInputFetcher.releaseBlockedWindows(readyWindows);
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {
    for (WindowedValue<InputT> elem : compressedElem.explodeWindows()) {
      if (!sideInputFetcher.storeIfBlocked(elem)) {
        simpleDoFnRunner.processElement(elem);
      }
    }
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain,
      CausedByDrain causedByDrain) {
    @SuppressWarnings("unchecked")
    StateNamespace stateNamespace = StateNamespaces.window(windowCoder, (W) window);

    TimerInternals.TimerData timerData =
        TimerInternals.TimerData.of(
            timerId,
            timerFamilyId,
            stateNamespace,
            timestamp,
            outputTimestamp,
            timeDomain,
            causedByDrain);
    // It's unclear what would cause storeIfBlocked to return true. A timer can only be set from an
    // element or another
    // timer processing. The fact that the timer was set implies that the side input was already
    // available, allowing the element to be processed.
    // However, there are many layers of caching in side inputs, so I can't rule this out right now.
    // In addition, the runner could conceivably elide the buffering if it notices that processElement doesn't
    // observe the side input (i.e. no SideInput or ProcessContext argument), in which case we will need to buffer
    // the timer.
    if (!sideInputFetcher.storeIfBlocked(timerData)) {
      simpleDoFnRunner.onTimer(
          timerId,
          timerFamilyId,
          key,
          window,
          timestamp,
          outputTimestamp,
          timeDomain,
          causedByDrain);
    }
  }

  @Override
  public void finishBundle() {
    simpleDoFnRunner.finishBundle();
    sideInputFetcher.persist();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    // This definitely can happen, since the onWindowExpiration timer isn't set in response to element processing.
    if (!sideInputFetcher.storeWindowExpirationIfBlocked(window, timestamp, key)) {
      simpleDoFnRunner.onWindowExpiration(window, timestamp, key);
    }
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return simpleDoFnRunner.getFn();
  }
}
