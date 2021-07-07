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
package org.apache.beam.fn.harness.control;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;

/**
 * Processes {@link BeamFnApi.InstructionRequest}'s {@link BeamFnApi.HarnessMonitoringInfosResponse}
 *
 * <p>These instructions are not associated with the currently processed bundles. They return
 * MonitoringInfos payloads for "process-wide" metrics, which return metric values calculated over
 * the life of the process.
 */
public class HarnessMonitoringInfosInstructionHandler {

  private final ShortIdMap metricsShortIds;

  public HarnessMonitoringInfosInstructionHandler(ShortIdMap metricsShortIds) {
    this.metricsShortIds = metricsShortIds;
  }

  public BeamFnApi.InstructionResponse.Builder harnessMonitoringInfos(
      BeamFnApi.InstructionRequest request) {
    BeamFnApi.HarnessMonitoringInfosResponse.Builder response =
        BeamFnApi.HarnessMonitoringInfosResponse.newBuilder();
    MetricsContainer container = MetricsEnvironment.getProcessWideContainer();
    if (container != null && container instanceof MetricsContainerImpl) {
      response.putAllMonitoringData(
          ((MetricsContainerImpl) container).getMonitoringData(this.metricsShortIds));
    }
    return BeamFnApi.InstructionResponse.newBuilder().setHarnessMonitoringInfos(response);
  }
}
