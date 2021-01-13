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
package org.apache.beam.runners.spark.structuredstreaming.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test BeamMetric. */
@RunWith(JUnit4.class)
public class BeamMetricTest {
  @Test
  public void testRenderName() {
    MetricResult<Object> metricResult =
        MetricResult.create(
            MetricKey.create(
                "myStep.one.two(three)", MetricName.named("myNameSpace//", "myName()")),
            123,
            456);
    String renderedName = new SparkBeamMetric().renderName(metricResult);
    assertThat(
        "Metric name was not rendered correctly",
        renderedName,
        equalTo("myStep_one_two_three.myNameSpace__.myName__"));
  }
}
