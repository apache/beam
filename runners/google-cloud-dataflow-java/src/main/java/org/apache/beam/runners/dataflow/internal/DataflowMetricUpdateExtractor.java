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
package org.apache.beam.runners.dataflow.internal;

import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Methods for extracting the values of an {@link Aggregator} from a collection of {@link
 * MetricUpdate MetricUpdates}.
 */
public final class DataflowMetricUpdateExtractor {
  private static final String STEP_NAME_CONTEXT_KEY = "step";
  private static final String IS_TENTATIVE_KEY = "tentative";

  private DataflowMetricUpdateExtractor() {
    // Do not instantiate.
  }

  /**
   * Extract the values of the provided {@link Aggregator} at each {@link PTransform} it was used in
   * according to the provided {@link DataflowAggregatorTransforms} from the given list of {@link
   * MetricUpdate MetricUpdates}.
   */
  public static <OutputT> Map<String, OutputT> fromMetricUpdates(Aggregator<?, OutputT> aggregator,
      DataflowAggregatorTransforms aggregatorTransforms, List<MetricUpdate> metricUpdates) {
    Map<String, OutputT> results = new HashMap<>();
    if (metricUpdates == null) {
      return results;
    }

    String aggregatorName = aggregator.getName();
    Collection<String> aggregatorSteps = aggregatorTransforms.getAggregatorStepNames(aggregator);

    for (MetricUpdate metricUpdate : metricUpdates) {
      MetricStructuredName metricStructuredName = metricUpdate.getName();
      Map<String, String> context = metricStructuredName.getContext();
      if (metricStructuredName.getName().equals(aggregatorName) && context != null
          && aggregatorSteps.contains(context.get(STEP_NAME_CONTEXT_KEY))) {
        AppliedPTransform<?, ?, ?> transform =
            aggregatorTransforms.getAppliedTransformForStepName(
                context.get(STEP_NAME_CONTEXT_KEY));
        String fullName = transform.getFullName();
        // Prefer the tentative (fresher) value if it exists.
        if (Boolean.parseBoolean(context.get(IS_TENTATIVE_KEY)) || !results.containsKey(fullName)) {
          results.put(fullName, toValue(aggregator, metricUpdate));
        }
      }
    }

    return results;

  }

  private static <OutputT> OutputT toValue(
      Aggregator<?, OutputT> aggregator, MetricUpdate metricUpdate) {
    CombineFn<?, ?, OutputT> combineFn = aggregator.getCombineFn();
    Class<? super OutputT> outputType = combineFn.getOutputType().getRawType();

    if (outputType.equals(Long.class)) {
      @SuppressWarnings("unchecked")
      OutputT asLong = (OutputT) Long.valueOf(toNumber(metricUpdate).longValue());
      return asLong;
    }
    if (outputType.equals(Integer.class)) {
      @SuppressWarnings("unchecked")
      OutputT asInt = (OutputT) Integer.valueOf(toNumber(metricUpdate).intValue());
      return asInt;
    }
    if (outputType.equals(Double.class)) {
      @SuppressWarnings("unchecked")
      OutputT asDouble = (OutputT) Double.valueOf(toNumber(metricUpdate).doubleValue());
      return asDouble;
    }
    throw new UnsupportedOperationException(
        "Unsupported Output Type " + outputType + " in aggregator " + aggregator);
  }

  private static Number toNumber(MetricUpdate update) {
    if (update.getScalar() instanceof Number) {
      return (Number) update.getScalar();
    }
    throw new IllegalArgumentException(
        "Metric Update " + update + " does not have a numeric scalar");
  }
}
