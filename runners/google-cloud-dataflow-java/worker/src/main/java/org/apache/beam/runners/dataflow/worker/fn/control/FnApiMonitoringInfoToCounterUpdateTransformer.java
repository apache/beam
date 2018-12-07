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
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;

/**
 * Generic MonitoringInfo to CounterUpdate transformer for FnApi.
 *
 * Delegates work to other transformers implementations for relevant counter types.
 */
public class FnApiMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  final UserMonitoringInfoToCounterUpdateTransformer userCounterTransformer;
  final Map<String, MonitoringInfoToCounterUpdateTransformer> counterTransformers = new HashMap<>();

  public FnApiMonitoringInfoToCounterUpdateTransformer(
      Map<String, DataflowStepContext> stepContextMap) {
    SpecMonitoringInfoValidator specValidator = new SpecMonitoringInfoValidator();
    this.userCounterTransformer =
        new UserMonitoringInfoToCounterUpdateTransformer(specValidator, stepContextMap);

    MSecMonitoringInfoToCounterUpdateTransformer msecTransformer =
        new MSecMonitoringInfoToCounterUpdateTransformer(specValidator, stepContextMap);
    for (String urn : msecTransformer.getSupportedUrns()) {
      this.counterTransformers.put(urn, msecTransformer);
    }
  }

  /** Allows for injection of user and generic counter transformers for more convenient testing. */
  @VisibleForTesting
  public FnApiMonitoringInfoToCounterUpdateTransformer(
      UserMonitoringInfoToCounterUpdateTransformer userCounterTransformer,
      Map<String, MonitoringInfoToCounterUpdateTransformer> counterTransformers) {
    this.userCounterTransformer = userCounterTransformer;
    this.counterTransformers.putAll(counterTransformers);
  }

  @Override
  public CounterUpdate transform(MonitoringInfo src) {
    String urn = src.getUrn();
    if (urn.startsWith(userCounterTransformer.getSupportedUrnPrefix())) {
      return userCounterTransformer.transform(src);
    }

    MonitoringInfoToCounterUpdateTransformer transformer = counterTransformers.get(urn);
    return transformer == null ? null : transformer.transform(src);
  }
}
