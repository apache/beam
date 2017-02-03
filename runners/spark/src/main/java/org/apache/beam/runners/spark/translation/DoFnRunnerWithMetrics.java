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

package org.apache.beam.runners.spark.translation;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.spark.metrics.SparkMetricsContainer;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.Accumulator;
import org.joda.time.Instant;


/**
 * DoFnRunner decorator which registers {@link org.apache.beam.sdk.metrics.MetricsContainer}.
 */
class DoFnRunnerWithMetrics<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> delegate;
  private final String stepName;
  private final Accumulator<SparkMetricsContainer> metricsAccum;

  DoFnRunnerWithMetrics(
      String stepName,
      DoFnRunner<InputT, OutputT> delegate,
      Accumulator<SparkMetricsContainer> metricsAccum) {
    this.delegate = delegate;
    this.stepName = stepName;
    this.metricsAccum = metricsAccum;
  }

  @Override
  public void startBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer())) {
      delegate.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(final WindowedValue<InputT> elem) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer())) {
      delegate.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onTimer(final String timerId, final BoundedWindow window, final Instant timestamp,
                      final TimeDomain timeDomain) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer())) {
      delegate.onTimer(timerId, window, timestamp, timeDomain);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer())) {
      delegate.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private MetricsContainer metricsContainer() {
    return metricsAccum.localValue().getContainer(stepName);
  }
}
