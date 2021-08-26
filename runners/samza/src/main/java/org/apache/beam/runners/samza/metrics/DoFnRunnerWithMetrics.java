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
package org.apache.beam.runners.samza.metrics;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * {@link DoFnRunner} wrapper with metrics. The class uses {@link SamzaMetricsContainer} to keep
 * BEAM metrics results and update Samza metrics.
 */
public class DoFnRunnerWithMetrics<InT, OutT> implements DoFnRunner<InT, OutT> {
  private final DoFnRunner<InT, OutT> underlying;
  private final SamzaMetricsContainer metricsContainer;
  private final FnWithMetricsWrapper metricsWrapper;

  private DoFnRunnerWithMetrics(
      DoFnRunner<InT, OutT> underlying, SamzaMetricsContainer metricsContainer, String stepName) {
    this.underlying = underlying;
    this.metricsContainer = metricsContainer;
    this.metricsWrapper = new FnWithMetricsWrapper(metricsContainer, stepName);
  }

  public static <InT, OutT> DoFnRunner<InT, OutT> wrap(
      DoFnRunner<InT, OutT> doFnRunner, SamzaMetricsContainer metricsContainer, String stepName) {
    return new DoFnRunnerWithMetrics<>(doFnRunner, metricsContainer, stepName);
  }

  @Override
  public void startBundle() {
    withMetrics(underlying::startBundle, false);
  }

  @Override
  public void processElement(WindowedValue<InT> elem) {
    withMetrics(() -> underlying.processElement(elem), false);
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
    withMetrics(
        () ->
            underlying.onTimer(
                timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain),
        false);
  }

  @Override
  public void finishBundle() {
    withMetrics(underlying::finishBundle, true);
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    underlying.onWindowExpiration(window, timestamp, key);
  }

  @Override
  public DoFn<InT, OutT> getFn() {
    return underlying.getFn();
  }

  private void withMetrics(Runnable runnable, boolean shouldUpdateMetrics) {
    try {
      metricsWrapper.wrap(
          () -> {
            runnable.run();
            return (Void) null;
          },
          shouldUpdateMetrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
