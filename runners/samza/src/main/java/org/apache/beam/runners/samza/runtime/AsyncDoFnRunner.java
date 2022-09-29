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
  private final boolean isStateful;
  private final KeyedOutputFutures<Object, OutT> keyedOutputFutures;

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
    this.keyedOutputFutures = isStateful ? new KeyedOutputFutures<>() : null;
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
      CompletableFuture<Collection<WindowedValue<OutT>>> prevOutputFuture) {

    final CompletableFuture<Collection<WindowedValue<OutT>>> prevFuture =
        prevOutputFuture == null
            ? CompletableFuture.completedFuture(Collections.emptyList())
            : prevOutputFuture;

    // For stateful processing, we chain the processing of the elem to the completion of
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

    // Track the latest outputFuture for key
    keyedOutputFutures.put(key, outputFutures);

    // Remove the outputFuture from the map once it's complete
    return outputFutures.thenApply(
        output -> {
          keyedOutputFutures.remove(key, outputFutures);
          return output;
        });
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
    return ((KV) elem.getValue()).getKey();
  }

  private static class KeyedOutputFutures<K, OutT> {
    private final Map<K, CompletableFuture<Collection<WindowedValue<OutT>>>> keyToOutputFutures =
        new ConcurrentHashMap<>();

    CompletableFuture<Collection<WindowedValue<OutT>>> get(K key) {
      return keyToOutputFutures.get(key);
    }

    void put(K key, CompletableFuture<Collection<WindowedValue<OutT>>> outputFuture) {
      keyToOutputFutures.put(key, outputFuture);
    }

    void remove(K key, CompletableFuture<Collection<WindowedValue<OutT>>> outputFuture) {
      keyToOutputFutures.remove(key, outputFuture);
    }
  }
}
