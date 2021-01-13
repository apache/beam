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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/** DoFnRunner decorator which registers {@link MetricsContainerImpl}. */
class DoFnRunnerWithMetrics<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> delegate;
  private final String stepName;
  private final MetricsContainerStepMapAccumulator metricsAccum;

  DoFnRunnerWithMetrics(
      String stepName,
      DoFnRunner<InputT, OutputT> delegate,
      MetricsContainerStepMapAccumulator metricsAccum) {
    this.delegate = delegate;
    this.stepName = stepName;
    this.metricsAccum = metricsAccum;
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return delegate.getFn();
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
  public <KeyT> void onTimer(
      final String timerId,
      final String timerFamilyId,
      KeyT key,
      final BoundedWindow window,
      final Instant timestamp,
      final Instant outputTimestamp,
      final TimeDomain timeDomain) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer())) {
      delegate.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
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

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    delegate.onWindowExpiration(window, timestamp, key);
  }

  private MetricsContainer metricsContainer() {
    return metricsAccum.value().getContainer(stepName);
  }
}
