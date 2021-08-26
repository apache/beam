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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/** Tests for {@link LabeledMetrics}. */
public class LabeledMetricsTest implements Serializable {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCounterDoesNotFailOperationsWhenNoMetricsContainerPresent() {
    MetricsEnvironment.setCurrentContainer(null);
    assertNull(MetricsEnvironment.getCurrentContainer());
    HashMap<String, String> labels = new HashMap<String, String>();
    String urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);

    Counter counter = LabeledMetrics.counter(name);
    counter.inc();
    counter.inc(5L);
    counter.dec();
    counter.dec(5L);
  }

  @Test
  public void testOperationsUpdateCounterFromContainerWhenContainerIsPresent() {
    HashMap<String, String> labels = new HashMap<String, String>();
    String urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);

    MetricsContainer mockContainer = Mockito.mock(MetricsContainer.class);
    Counter mockCounter = Mockito.mock(Counter.class);
    when(mockContainer.getCounter(name)).thenReturn(mockCounter);

    Counter counter = LabeledMetrics.counter(name);

    MetricsEnvironment.setCurrentContainer(mockContainer);
    counter.inc();
    verify(mockCounter).inc(1);

    counter.inc(47L);
    verify(mockCounter).inc(47);

    counter.dec(5L);
    verify(mockCounter).inc(-5);
  }
}
