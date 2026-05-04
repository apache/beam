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

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.state.TimeDomain;
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
  private final StreamingSideInputProcessor<InputT, OutputT, W> sideInputProcessor;

  public StreamingSideInputDoFnRunner(
      DoFnRunner<InputT, OutputT> simpleDoFnRunner,
      StreamingSideInputFetcher<InputT, W> sideInputFetcher) {
    this.simpleDoFnRunner = simpleDoFnRunner;
    this.sideInputProcessor = new StreamingSideInputProcessor<>(sideInputFetcher);
  }

  @Override
  public void startBundle() {
    simpleDoFnRunner.startBundle();
    Iterable<WindowedValue<InputT>> unblocked = sideInputProcessor.tryUnblockElements();
    for (WindowedValue<InputT> elem : unblocked) {
      simpleDoFnRunner.processElement(elem);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {
    Iterable<WindowedValue<InputT>> unblocked =
        sideInputProcessor.handleProcessElement(compressedElem);
    for (WindowedValue<InputT> elem : unblocked) {
      simpleDoFnRunner.processElement(elem);
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
    throw new UnsupportedOperationException(
        "Attempt to deliver a timer to a DoFn, but timers are not supported in Dataflow.");
  }

  @Override
  public void finishBundle() {
    simpleDoFnRunner.finishBundle();
    sideInputProcessor.handleFinishBundle();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    simpleDoFnRunner.onWindowExpiration(window, timestamp, key);
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return simpleDoFnRunner.getFn();
  }
}
