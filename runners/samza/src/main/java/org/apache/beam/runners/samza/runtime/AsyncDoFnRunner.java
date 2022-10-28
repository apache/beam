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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
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

  // A dummy key to represent null keys
  private static final Object NULL_KEY = new Object();

  private final DoFnRunner<InT, OutT> underlying;
  private final ExecutorService executor;
  private final OpEmitter<OutT> emitter;
  private final FutureCollector<OutT> futureCollector;
  private final boolean isStateful;

  /**
   * This map keeps track of the last outputFutures for a certain key. When the next element of the
   * key comes in, its outputFutures will be chained from the last outputFutures in the map. When
   * all futures of a key have been complete, the key entry will be removed. The map is bounded by
   * (bundle size * 2).
   */
  private final Map<Object, CompletableFuture<Collection<WindowedValue<OutT>>>> keyedOutputFutures;

  public static <InT, OutT> AsyncDoFnRunner<InT, OutT> create(
      DoFnRunner<InT, OutT> runner,
      OpEmitter<OutT> emitter,
      FutureCollector<OutT> futureCollector,
      boolean isStateful,
      SamzaPipelineOptions options) {

    LOG.info("Run DoFn with " + AsyncDoFnRunner.class.getName());
    return new AsyncDoFnRunner<>(runner, emitter, futureCollector, isStateful, options);
  }

  private AsyncDoFnRunner(
      DoFnRunner<InT, OutT> runner,
      OpEmitter<OutT> emitter,
      FutureCollector<OutT> futureCollector,
      boolean isStateful,
      SamzaPipelineOptions options) {
    this.underlying = runner;
    this.executor = options.getExecutorServiceForProcessElement();
    this.emitter = emitter;
    this.futureCollector = futureCollector;
    this.isStateful = isStateful;
    this.keyedOutputFutures = new ConcurrentHashMap<>();
  }

  @Override
  public void startBundle() {
    underlying.startBundle();
  }

  @Override
  public void processElement(WindowedValue<InT> elem) {
    final CompletableFuture<Collection<WindowedValue<OutT>>> outputFutures =
        isStateful ? processStateful(elem) : processElement(elem, null);

    futureCollector.addAll(outputFutures);
  }

  private CompletableFuture<Collection<WindowedValue<OutT>>> processElement(
      WindowedValue<InT> elem,
      @Nullable CompletableFuture<Collection<WindowedValue<OutT>>> prevOutputFuture) {

    final CompletableFuture<Collection<WindowedValue<OutT>>> prevFuture =
        prevOutputFuture == null
            ? CompletableFuture.completedFuture(Collections.emptyList())
            : prevOutputFuture;

    // For ordering by key, we chain the processing of the elem to the completion of
    // the previous output of the same key
    return prevFuture.thenApplyAsync(
        x -> {
          underlying.processElement(elem);

          return emitter.collectOutput().stream()
              .map(OpMessage::getElement)
              .collect(Collectors.toList());
        },
        executor);
  }

  private CompletableFuture<Collection<WindowedValue<OutT>>> processStateful(
      WindowedValue<InT> elem) {
    final Object key = getKey(elem);

    final CompletableFuture<Collection<WindowedValue<OutT>>> outputFutures =
        processElement(elem, keyedOutputFutures.get(key));

    // Update the latest outputFuture for key
    keyedOutputFutures.put(key, outputFutures);

    // Remove the outputFuture from the map once it's complete.
    // This ensures the map will be cleaned up immediately.
    return outputFutures.thenApply(
        output -> {
          // Under the condition that the outputFutures has not been updated
          keyedOutputFutures.remove(key, outputFutures);
          return output;
        });
  }

  /** Package private for testing. */
  boolean hasOutputFuturesForKey(Object key) {
    return keyedOutputFutures.containsKey(key);
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

  private Object getKey(WindowedValue<InT> elem) {
    KV<?, ?> kv = (KV<?, ?>) elem.getValue();
    if (kv == null) {
      return NULL_KEY;
    } else {
      Object key = kv.getKey();
      return key == null ? NULL_KEY : key;
    }
  }
}
