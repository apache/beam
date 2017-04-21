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
package org.apache.beam.runners.flink.metrics;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.joda.time.Instant;

/**
 * {@link DoFnRunner} decorator which registers
 * {@link org.apache.beam.sdk.metrics.MetricsContainer}. It updates metrics to Flink metrics and
 * accumulators in {@link #finishBundle()}.
 */
public class DoFnRunnerWithMetricsUpdate<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private final FlinkMetricContainer container;
  private final DoFnRunner<InputT, OutputT> delegate;

  public DoFnRunnerWithMetricsUpdate(
      String stepName,
      DoFnRunner<InputT, OutputT> delegate,
      RuntimeContext runtimeContext) {
    this.delegate = delegate;
    container = new FlinkMetricContainer(stepName, runtimeContext);
  }

  @Override
  public void startBundle() {
    try (Closeable ignored =
             MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer())) {
      delegate.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(final WindowedValue<InputT> elem) {
    try (Closeable ignored =
             MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer())) {
      delegate.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onTimer(final String timerId, final BoundedWindow window, final Instant timestamp,
                      final TimeDomain timeDomain) {
    try (Closeable ignored =
             MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer())) {
      delegate.onTimer(timerId, window, timestamp, timeDomain);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishBundle() {
    try (Closeable ignored =
             MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer())) {
      delegate.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // update metrics
    container.updateMetrics();
  }
}
