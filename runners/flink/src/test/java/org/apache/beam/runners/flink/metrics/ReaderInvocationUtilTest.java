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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/** Tests {@link ReaderInvocationUtil}. */
public class ReaderInvocationUtilTest {

  private static final String STEP = "step";
  private static final long INTERVAL_NANOS =
      TimeUnit.MILLISECONDS.toNanos(ReaderInvocationUtil.FLINK_METRICS_UPDATE_INTERVAL_MILLIS);

  private final AtomicLong clock = new AtomicLong();
  private FlinkMetricContainerBase container;
  private Source.Reader<String> reader;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    container = mock(FlinkMetricContainerBase.class);
    when(container.getMetricsContainer(STEP)).thenReturn(new MetricsContainerImpl(STEP));
    reader = mock(Source.Reader.class);
  }

  private ReaderInvocationUtil<String, Source.Reader<String>> invoker(boolean disableMetrics) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setDisableMetrics(disableMetrics);
    return new ReaderInvocationUtil<>(STEP, options, container, clock::get);
  }

  @Test
  public void updatesFlinkMetricsAtMostOncePerInterval() throws IOException {
    when(reader.advance()).thenReturn(true);
    ReaderInvocationUtil<String, Source.Reader<String>> invoker = invoker(false);

    // The first call registers the metrics in Flink.
    invoker.invokeAdvance(reader);
    verify(container, times(1)).updateMetrics(STEP);

    for (int i = 0; i < 1000; i++) {
      invoker.invokeAdvance(reader);
    }
    verify(container, times(1)).updateMetrics(STEP);

    clock.addAndGet(INTERVAL_NANOS);
    invoker.invokeAdvance(reader);
    invoker.invokeAdvance(reader);
    verify(container, times(2)).updateMetrics(STEP);

    clock.addAndGet(INTERVAL_NANOS);
    invoker.invokeAdvance(reader);
    verify(container, times(3)).updateMetrics(STEP);
  }

  @Test
  public void updatesFlinkMetricsWhenTheReaderHasNoData() throws IOException {
    when(reader.start()).thenReturn(true);
    when(reader.advance()).thenReturn(true, true, false);
    ReaderInvocationUtil<String, Source.Reader<String>> invoker = invoker(false);

    invoker.invokeStart(reader);
    verify(container, times(1)).updateMetrics(STEP);

    invoker.invokeAdvance(reader);
    invoker.invokeAdvance(reader);
    verify(container, times(1)).updateMetrics(STEP);

    invoker.invokeAdvance(reader);
    verify(container, times(2)).updateMetrics(STEP);
  }

  @Test
  public void doesNotUpdateFlinkMetricsWhenDisabled() throws IOException {
    when(reader.start()).thenReturn(false);
    when(reader.advance()).thenReturn(false);
    ReaderInvocationUtil<String, Source.Reader<String>> invoker = invoker(true);

    invoker.invokeStart(reader);
    clock.addAndGet(INTERVAL_NANOS);
    invoker.invokeAdvance(reader);

    verify(container, never()).updateMetrics(STEP);
    verify(container, never()).getMetricsContainer(STEP);
  }
}
