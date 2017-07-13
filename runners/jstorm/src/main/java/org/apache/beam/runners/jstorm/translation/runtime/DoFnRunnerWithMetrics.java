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
package org.apache.beam.runners.jstorm.translation.runtime;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * DoFnRunner decorator which registers {@link MetricsContainer}.
 */
public class DoFnRunnerWithMetrics<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private final String stepName;
  private final DoFnRunner<InputT, OutputT> delegate;
  private final MetricsReporter metricsReporter;

  DoFnRunnerWithMetrics(
      String stepName,
      DoFnRunner<InputT, OutputT> delegate,
      MetricsReporter metricsReporter) {
    this.stepName = checkNotNull(stepName, "stepName");
    this.delegate = checkNotNull(delegate, "delegate");
    this.metricsReporter = checkNotNull(metricsReporter, "metricsReporter");
  }

  @Override
  public void startBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      delegate.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      delegate.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      delegate.onTimer(timerId, window, timestamp, timeDomain);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      delegate.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    metricsReporter.updateMetrics();
  }
}
