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
package org.apache.beam.runners.samza.runtime;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class AsyncDoFnRunner<InT, OutT> implements DoFnRunner<InT, OutT> {
  private final DoFnRunner<InT, OutT> underlying;
  private final ExecutorService executor;
  private final OpEmitter<OutT> emitter;

  public AsyncDoFnRunner(
      DoFnRunner<InT, OutT> runner, OpEmitter<OutT> emitter, SamzaPipelineOptions options) {
    this.underlying = runner;
    // TODO: change to key-based thread pool if needed
    this.executor = Executors.newFixedThreadPool(options.getBundleThreadNum());
    this.emitter = emitter;
  }

  @Override
  public void startBundle() {
    underlying.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InT> elem) {
    final CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              int r = new Random().nextInt(10);
              try {
                Thread.sleep(r);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }

              underlying.processElement(elem);
            },
            executor);

    final CompletableFuture<Collection<WindowedValue<OutT>>> outputFutures =
        future.thenApply(
            x ->
                emitter.collectOutput().stream()
                    .map(OpMessage::getElement)
                    .collect(Collectors.toList()));

    emitter.emitFuture(outputFutures);
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
    underlying.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
  }

  @Override
  public void finishBundle() {
    underlying.finishBundle();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    underlying.onWindowExpiration(window, timestamp, key);
  }

  @Override
  public DoFn<InT, OutT> getFn() {
    return underlying.getFn();
  }
}
