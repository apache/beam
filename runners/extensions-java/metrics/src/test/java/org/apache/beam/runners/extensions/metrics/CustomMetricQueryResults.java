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
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.joda.time.Instant;

/** Test class to be used as a input to {@link MetricsSink} implementations tests. */
class CustomMetricQueryResults implements MetricQueryResults {

  private final boolean isCommittedSupported;

  CustomMetricQueryResults(boolean isCommittedSupported) {
    this.isCommittedSupported = isCommittedSupported;
  }

  @Override
  public List<MetricResult<Long>> getCounters() {
    return Collections.singletonList(
        new MetricResult<Long>() {

          @Override
          public MetricName getName() {
            return MetricName.named("ns1", "n1");
          }

          @Override
          public String getStep() {
            return "s1";
          }

          @Override
          public Long getCommitted() {
            if (!isCommittedSupported) {
              // This is what getCommitted code is like for AccumulatedMetricResult on runners
              // that do not support committed metrics
              throw new UnsupportedOperationException(
                  "This runner does not currently support committed"
                      + " metrics results. Please use 'attempted' instead.");
            }
            return 10L;
          }

          @Override
          public Long getAttempted() {
            return 20L;
          }
        });
  }

  @Override
  public List<MetricResult<DistributionResult>> getDistributions() {
    return Collections.singletonList(
        new MetricResult<DistributionResult>() {

          @Override
          public MetricName getName() {
            return MetricName.named("ns1", "n2");
          }

          @Override
          public String getStep() {
            return "s2";
          }

          @Override
          public DistributionResult getCommitted() {
            if (!isCommittedSupported) {
              // This is what getCommitted code is like for AccumulatedMetricResult on runners
              // that do not support committed metrics
              throw new UnsupportedOperationException(
                  "This runner does not currently support committed"
                      + " metrics results. Please use 'attempted' instead.");
            }
            return DistributionResult.create(10L, 2L, 5L, 8L);
          }

          @Override
          public DistributionResult getAttempted() {
            return DistributionResult.create(25L, 4L, 3L, 9L);
          }
        });
  }

  @Override
  public List<MetricResult<GaugeResult>> getGauges() {
    return Collections.singletonList(
        new MetricResult<GaugeResult>() {

          @Override
          public MetricName getName() {
            return MetricName.named("ns1", "n3");
          }

          @Override
          public String getStep() {
            return "s3";
          }

          @Override
          public GaugeResult getCommitted() {
            if (!isCommittedSupported) {
              // This is what getCommitted code is like for AccumulatedMetricResult on runners
              // that do not support committed metrics
              throw new UnsupportedOperationException(
                  "This runner does not currently support committed"
                      + " metrics results. Please use 'attempted' instead.");
            }
            return GaugeResult.create(100L, new Instant(345862800L));
          }

          @Override
          public GaugeResult getAttempted() {
            return GaugeResult.create(120L, new Instant(345862800L));
          }
        });
  }
}
