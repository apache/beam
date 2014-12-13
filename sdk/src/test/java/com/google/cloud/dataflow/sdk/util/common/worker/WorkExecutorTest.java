/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import com.google.cloud.dataflow.sdk.util.common.Metric;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Unit tests for {@link WorkExecutor}.
 */
@RunWith(JUnit4.class)
public class WorkExecutorTest {
  private WorkExecutor mapWorker;
  private WorkExecutor seqMapWorker;

  @Before
  public void setUp() {
    mapWorker = new MapTaskExecutor(null, null, null);
  }

  @Test
  public void testMapTaskGetOutputMetrics() {
    Collection<Metric<?>> metrics = mapWorker.getOutputMetrics();
    verifyOutputMetrics(metrics);
  }

  private void verifyOutputMetrics(Collection<Metric<?>> metrics) {
    Collection<String> metricNames = new ArrayList<>();
    for (Metric<?> metric : metrics) {
      metricNames.add(metric.getName());
    }
    Assert.assertThat(metricNames, containsInAnyOrder("CPU"));
  }
}
