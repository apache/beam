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
package org.apache.beam.runners.extensions.metrics;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;

/** Test class to be used as a input to {@link MetricsSink} implementations tests. */
class CustomMetricQueryResults extends MetricQueryResults {

  private final boolean isCommittedSupported;

  CustomMetricQueryResults(boolean isCommittedSupported) {
    this.isCommittedSupported = isCommittedSupported;
  }

  public static final String NAMESPACE = "ns1";

  private <T> List<MetricResult<T>> makeResults(
      String step, String name, T committed, T attempted) {
    MetricName metricName = MetricName.named(NAMESPACE, name);
    MetricKey key = MetricKey.create(step, metricName);
    return Collections.singletonList(
        isCommittedSupported
            ? MetricResult.create(key, committed, attempted)
            : MetricResult.attempted(key, attempted));
  }

  @Override
  public List<MetricResult<Long>> getCounters() {
    return makeResults("s1", "n1", 10L, 20L);
  }

  @Override
  public List<MetricResult<DistributionResult>> getDistributions() {
    return makeResults(
        "s2",
        "n2",
        DistributionResult.create(10L, 2L, 5L, 8L),
        DistributionResult.create(25L, 4L, 3L, 9L));
  }

  @Override
  public List<MetricResult<GaugeResult>> getGauges() {
    return makeResults(
        "s3",
        "n3",
        GaugeResult.create(100L, new Instant(345862800L)),
        GaugeResult.create(120L, new Instant(345862800L)));
  }

  @Override
  public Iterable<MetricResult<StringSetResult>> getStringSets() {
    return makeResults(
        "s3",
        "n3",
        StringSetResult.create(ImmutableSet.of("ab")),
        StringSetResult.create(ImmutableSet.of("cd")));
  }

  @Override
  public Iterable<MetricResult<HistogramData>> getPerWorkerHistograms() {
    return Collections.emptyList();
  }
}
