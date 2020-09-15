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
package org.apache.beam.runners.dataflow.worker.fn.control;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Generic MonitoringInfo to CounterUpdate transformer for FnApi.
 *
 * <p>Delegates work to other transformers implementations for relevant counter types.
 */
public class FnApiMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  final Map<String, MonitoringInfoToCounterUpdateTransformer> urnToCounterTransformers;

  public FnApiMonitoringInfoToCounterUpdateTransformer(
      Map<String, DataflowStepContext> stepContextMap,
      Map<String, NameContext> sdkPCollectionIdToNameContext) {
    SpecMonitoringInfoValidator specValidator = new SpecMonitoringInfoValidator();

    urnToCounterTransformers = new HashMap<>();

    UserMonitoringInfoToCounterUpdateTransformer userCounterTransformer =
        new UserMonitoringInfoToCounterUpdateTransformer(specValidator, stepContextMap);
    urnToCounterTransformers.put(
        UserMonitoringInfoToCounterUpdateTransformer.getSupportedUrn(), userCounterTransformer);

    UserDistributionMonitoringInfoToCounterUpdateTransformer userDistributionCounterTransformer =
        new UserDistributionMonitoringInfoToCounterUpdateTransformer(specValidator, stepContextMap);
    urnToCounterTransformers.put(
        UserDistributionMonitoringInfoToCounterUpdateTransformer.getSupportedUrn(),
        userDistributionCounterTransformer);

    ExecutionTimeMonitoringInfoToCounterUpdateTransformer msecTransformer =
        new ExecutionTimeMonitoringInfoToCounterUpdateTransformer(specValidator, stepContextMap);
    for (String urn : ExecutionTimeMonitoringInfoToCounterUpdateTransformer.getSupportedUrns()) {
      this.urnToCounterTransformers.put(urn, msecTransformer);
    }
    this.urnToCounterTransformers.put(
        ElementCountMonitoringInfoToCounterUpdateTransformer.getSupportedUrn(),
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            specValidator, sdkPCollectionIdToNameContext));
    this.urnToCounterTransformers.put(
        MeanByteCountMonitoringInfoToCounterUpdateTransformer.getSupportedUrn(),
        new MeanByteCountMonitoringInfoToCounterUpdateTransformer(
            specValidator, sdkPCollectionIdToNameContext));
  }

  /** Allows for injection of user and generic counter transformers for more convenient testing. */
  @VisibleForTesting
  public FnApiMonitoringInfoToCounterUpdateTransformer(
      Map<String, MonitoringInfoToCounterUpdateTransformer> urnToCounterTransformers) {
    this.urnToCounterTransformers = urnToCounterTransformers;
  }

  @Override
  public @Nullable CounterUpdate transform(MonitoringInfo src) {
    MonitoringInfoToCounterUpdateTransformer transformer =
        urnToCounterTransformers.get(src.getUrn());
    if (transformer == null) {
      return null;
    }
    return transformer.transform(src);
  }
}
