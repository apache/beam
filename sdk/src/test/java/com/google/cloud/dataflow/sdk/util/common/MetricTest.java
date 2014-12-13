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

package com.google.cloud.dataflow.sdk.util.common;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.util.common.Metric.DoubleMetric;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Metric}. */
@RunWith(JUnit4.class)
public class MetricTest {
  @Test
  public void testDoubleMetric() {
    String name = "metric-name";
    double value = 3.14;

    DoubleMetric doubleMetric = new DoubleMetric(name, value);

    assertEquals(name, doubleMetric.getName());
    assertEquals((Double) value, doubleMetric.getValue());
  }
}
