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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link DoFnRunner} adds the capability of executing the {@link
 * org.apache.beam.sdk.transforms.DoFn.ProcessElement} in the thread pool, and returns the future to
 * the collector for the underlying async execution.
 */
public class AsyncDoFnRunner<InT, OutT> implements DoFnRunner<InT, OutT> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDoFnRunner.class);

  private final DoFnRunner<InT, OutT> underlying;
  private final ExecutorService executor;
  private final OpEmitter<OutT> emitter;
  private final FutureCollector<OutT> futureCollector;

  public static <InT, OutT> AsyncDoFnRunner<InT, OutT> create(
      DoFnRunner<InT, OutT> runner,
      OpEmitter<OutT> emitter,
      FutureCollector<OutT> futureCollector,
      SamzaPipelineOptions options) {

    LOG.info("Run DoFn with " + AsyncDoFnRunner.class.getName());
    return new AsyncDoFnRunner<>(runner, emitter, futureCollector, options);
  }

  private AsyncDoFnRunner(
      DoFnRunner<InT, OutT> runner,
      OpEmitter<OutT> emitter,
      FutureCollector<OutT> futureCollector,
      SamzaPipelineOptions options) {
    this.underlying = runner;
    this.executor = options.getExecutorServiceForProcessElement();
    this.emitter = emitter;
    this.futureCollector = futureCollector;
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
              underlying.processElement(elem);
            },
            executor);

    final CompletableFuture<Collection<WindowedValue<OutT>>> outputFutures =
        future.thenApply(
            x ->
                emitter.collectOutput().stream()
                    .map(OpMessage::getElement)
                    .collect(Collectors.toList()));

    futureCollector.addAll(outputFutures);
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
