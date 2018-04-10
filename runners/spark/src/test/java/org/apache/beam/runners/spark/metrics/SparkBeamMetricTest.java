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

package org.apache.beam.runners.spark.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;
import org.junit.Test;


/**
 * Test SparkBeamMetric.
 */
public class SparkBeamMetricTest {
  @Test
  public void testRenderName() throws Exception {
    MetricResult<Object> metricResult = new MetricResult<Object>() {
      @Override
      public MetricName getName() {
        return MetricName.named("myNameSpace//", "myName()");
      }

      @Override
      public String getStep() {
        return "myStep.one.two(three)";
      }

      @Override
      public Object getCommitted() {
        return null;
      }

      @Override
      public Object getAttempted() {
        return null;
      }
    };
    String renderedName = new SparkBeamMetric().renderName(metricResult);
    assertThat("Metric name was not rendered correctly", renderedName,
        equalTo("myStep_one_two_three.myNameSpace__.myName__"));
  }
}
