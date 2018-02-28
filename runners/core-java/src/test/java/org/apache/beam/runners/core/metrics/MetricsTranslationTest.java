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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests for {@link MetricsTranslation}. */
@RunWith(Parameterized.class)
public class MetricsTranslationTest {

  // Transform names are arbitrary user-meaningful steps in processing
  private static final String TRANSFORM1 = "transform1";
  private static final String TRANSFORM2 = "transform2";

  // Namespaces correspond to different contexts for a metric
  private static final String NAMESPACE1 = "fakeNamespace1";
  private static final String NAMESPACE2 = "fakeNamespace2";

  // Names are what is being measured
  private static final String COUNTER_NAME1 = "elements";
  private static final String COUNTER_NAME2 = "dropped";
  private static final String DISTRIBUTION_NAME1 = "someMillis";
  private static final String DISTRIBUTION_NAME2 = "otherMillis";

  private static final BeamFnApi.Metrics.User.MetricName COUNTER_METRIC1 =
      BeamFnApi.Metrics.User.MetricName.newBuilder()
          .setNamespace(NAMESPACE1)
          .setName(COUNTER_NAME1)
          .build();

  private static final BeamFnApi.Metrics.User.MetricName COUNTER_METRIC2 =
      BeamFnApi.Metrics.User.MetricName.newBuilder()
          .setNamespace(NAMESPACE1)
          .setName(COUNTER_NAME2)
          .build();

  private static final BeamFnApi.Metrics.User.MetricName DISTRIBUTION_METRIC1 =
      BeamFnApi.Metrics.User.MetricName.newBuilder()
          .setNamespace(NAMESPACE2)
          .setName(DISTRIBUTION_NAME1)
          .build();

  private static final BeamFnApi.Metrics.User.MetricName DISTRIBUTION_METRIC2 =
      BeamFnApi.Metrics.User.MetricName.newBuilder()
          .setNamespace(NAMESPACE2)
          .setName(DISTRIBUTION_NAME2)
          .build();

  private static final BeamFnApi.Metrics.User DISTRIBUTION1 =
      BeamFnApi.Metrics.User.newBuilder()
          .setMetricName(DISTRIBUTION_METRIC1)
          .setDistributionData(
              BeamFnApi.Metrics.User.DistributionData.newBuilder()
                  .setCount(42)
                  .setSum(4839L)
                  .setMax(348L)
                  .setMin(12L))
          .build();

  private static final BeamFnApi.Metrics.User DISTRIBUTION2 =
      BeamFnApi.Metrics.User.newBuilder()
          .setMetricName(DISTRIBUTION_METRIC2)
          .setDistributionData(
              BeamFnApi.Metrics.User.DistributionData.newBuilder()
                  .setCount(3)
                  .setSum(49L)
                  .setMax(43L)
                  .setMin(1L))
          .build();

  private static final BeamFnApi.Metrics.User COUNTER1 =
      BeamFnApi.Metrics.User.newBuilder()
          .setMetricName(COUNTER_METRIC1)
          .setCounterData(BeamFnApi.Metrics.User.CounterData.newBuilder().setValue(92L))
          .build();

  private static final BeamFnApi.Metrics.User COUNTER2 =
      BeamFnApi.Metrics.User.newBuilder()
          .setMetricName(COUNTER_METRIC2)
          .setCounterData(BeamFnApi.Metrics.User.CounterData.newBuilder().setValue(0L))
          .build();

  @Parameterized.Parameters
  public static Iterable<Object[]> testInstances() {
    return ImmutableList.<Object[]>builder()
        .add(
            new Object[] {
              ImmutableMap.builder().put(TRANSFORM1, ImmutableList.of(DISTRIBUTION1)).build()
            })
        .add(
            new Object[] {
              ImmutableMap.builder()
                  .put(TRANSFORM1, ImmutableList.of(DISTRIBUTION1, COUNTER1))
                  .build()
            })
        .add(
            new Object[] {
              ImmutableMap.builder()
                  .put(TRANSFORM1, ImmutableList.of(DISTRIBUTION1, COUNTER1))
                  .put(TRANSFORM2, ImmutableList.of(COUNTER2))
                  .build()
            })
        .build();
  }

  @Parameterized.Parameter(0)
  public Map<String, Collection<BeamFnApi.Metrics.User>> fnMetrics;

  @Test
  public void testToFromProtoMetricUpdates() {
    ImmutableMap.Builder<String, Collection<BeamFnApi.Metrics.User>> result =
        ImmutableMap.builder();

    for (Map.Entry<String, Collection<BeamFnApi.Metrics.User>> entry : fnMetrics.entrySet()) {
      MetricUpdates updates =
          MetricsTranslation.metricUpdatesFromProto(entry.getKey(), entry.getValue());
      result.putAll(MetricsTranslation.metricUpdatesToProto(updates));
    }

    Map<String, Collection<BeamFnApi.Metrics.User>> backToProto = result.build();

    assertThat(backToProto.keySet(), equalTo(fnMetrics.keySet()));

    for (String ptransformName : backToProto.keySet()) {
      assertThat(
          backToProto.get(ptransformName),
          containsInAnyOrder(fnMetrics.get(ptransformName).toArray()));
    }
  }
}
