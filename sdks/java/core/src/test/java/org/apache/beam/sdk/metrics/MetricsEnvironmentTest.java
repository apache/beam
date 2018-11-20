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
package org.apache.beam.sdk.metrics;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link MetricsEnvironment}. */
@RunWith(JUnit4.class)
public class MetricsEnvironmentTest {
  @After
  public void teardown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Test
  public void testUsesAppropriateMetricsContainer() {
    Counter counter = Metrics.counter("ns", "name");

    MetricsContainer c1 = Mockito.mock(MetricsContainer.class);
    MetricsContainer c2 = Mockito.mock(MetricsContainer.class);
    Counter counter1 = Mockito.mock(Counter.class);
    Counter counter2 = Mockito.mock(Counter.class);
    when(c1.getCounter(MetricName.named("ns", "name"))).thenReturn(counter1);
    when(c2.getCounter(MetricName.named("ns", "name"))).thenReturn(counter2);

    MetricsEnvironment.setCurrentContainer(c1);
    counter.inc();
    MetricsEnvironment.setCurrentContainer(c2);
    counter.dec();
    MetricsEnvironment.setCurrentContainer(null);

    verify(counter1).inc(1L);
    verify(counter2).inc(-1L);
    verifyNoMoreInteractions(counter1, counter2);
  }

  @Test
  public void testBehavesWithoutMetricsContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
  }
}
