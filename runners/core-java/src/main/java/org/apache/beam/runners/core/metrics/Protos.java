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

import org.apache.beam.model.jobmanagement.v1.JobApiMetrics;
import org.apache.beam.model.jobmanagement.v1.JobApiMetrics.CounterResult;
import org.apache.beam.model.pipeline.v1.PipelineMetrics;
import org.apache.beam.model.pipeline.v1.PipelineMetrics.IntDistributionData;
import org.apache.beam.model.pipeline.v1.PipelineMetrics.MetricKey;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;

/**
 * Conversions between {@link org.apache.beam.sdk.metrics} classes and corresponding {@link
 * JobApiMetrics} and {@link PipelineMetrics} proto objects.
 */
public class Protos {

  public static MetricName toProto(PipelineMetrics.MetricName metricName) {
    return MetricName.named(metricName.getNamespace(), metricName.getName());
  }

  public static MetricKey keyFromProto(MetricResult<?> metricResult) {
    MetricName metricName = metricResult.getName();
    return MetricKey.newBuilder()
        .setStep(metricResult.getStep())
        .setMetricName(
            PipelineMetrics.MetricName.newBuilder()
                .setNamespace(metricName.getNamespace())
                .setName(metricName.getName()))
        .build();
  }

  public static JobApiMetrics.DistributionResult distributionToProto(
      MetricResult<DistributionResult> result) {
    JobApiMetrics.DistributionResult.Builder builder =
        JobApiMetrics.DistributionResult.newBuilder().setAttempted(toProto(result.getAttempted()));
    try {
      builder.setCommitted(toProto(result.getCommitted()));
    } catch (UnsupportedOperationException ignored) {
    }

    return builder.build();
  }

  public static IntDistributionData toProto(DistributionResult distributionResult) {
    return IntDistributionData.newBuilder()
        .setMin(distributionResult.getMin())
        .setMax(distributionResult.getMax())
        .setCount(distributionResult.getCount())
        .setSum(distributionResult.getSum())
        .build();
  }

  public static DistributionResult fromProto(IntDistributionData distributionData) {
    return DistributionResult.create(
        distributionData.getSum(),
        distributionData.getCount(),
        distributionData.getMin(),
        distributionData.getMax());
  }

  public static CounterResult counterToProto(MetricResult<Long> result) {
    CounterResult.Builder builder = CounterResult.newBuilder().setAttempted(result.getAttempted());
    try {
      builder.setCommitted(result.getCommitted());
    } catch (UnsupportedOperationException ignored) {
    }

    return builder.build();
  }
}
