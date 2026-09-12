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

import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
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
  private @Nullable StreamingSideInputProcessor<InputT, W> sideInputProcessor = null;
  private Supplier<StreamingSideInputFetcher<InputT, W>> sideInputFetcherSupplier;
  boolean activeKey = false;

  public StreamingSideInputDoFnRunner(
      DoFnRunner<InputT, OutputT> simpleDoFnRunner,
      Supplier<StreamingSideInputFetcher<InputT, W>> sideInputFetcherSupplier) {
    this.simpleDoFnRunner = simpleDoFnRunner;
    this.sideInputFetcherSupplier = sideInputFetcherSupplier;
  }

  @Override
  public void startBundle() {
    simpleDoFnRunner.startBundle();
    this.activeKey = false;
  }

  private void onNewKey() {
    this.sideInputProcessor = new StreamingSideInputProcessor<>(sideInputFetcherSupplier.get());
    sideInputProcessor.tryUnblockElements(
        unblocked -> {
          for (WindowedValue<InputT> elem : unblocked) {
            simpleDoFnRunner.processElement(elem);
          }
        });
    this.activeKey = true;
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {
    if (!activeKey) {
      onNewKey();
    }
    Preconditions.checkStateNotNull(sideInputProcessor);
    for (Iterator<? extends WindowedValue<InputT>> it =
            sideInputProcessor.handleProcessElement(compressedElem);
        it.hasNext(); ) {
      WindowedValue<InputT> elem = it.next();
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
  public <KeyT extends @Nullable Object> void finishKey(KeyT key) {
    if (!activeKey) {
      // This means that there were no elements for this key. Try to unblock any queued elements.
      onNewKey();
    }
    Preconditions.checkStateNotNull(sideInputProcessor).handleFinishKeyOrBundle();
    simpleDoFnRunner.finishKey(key);
    this.activeKey = false;
    this.sideInputProcessor = null;
  }

  @Override
  public void finishBundle() {
    simpleDoFnRunner.finishBundle();
    this.activeKey = false;
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
