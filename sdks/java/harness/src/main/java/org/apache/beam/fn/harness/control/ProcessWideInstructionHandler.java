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
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO can this be a static class?
public class ProcessWideInstructionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessWideInstructionHandler.class);

  public ProcessWideInstructionHandler() { }

  public BeamFnApi.InstructionResponse.Builder monitoringInfoMetadata(
      BeamFnApi.InstructionRequest request) {
    LOG.info("ajamato monitoringInfoMetadata request " + request.toString());
    BeamFnApi.MonitoringInfosMetadataResponse.Builder response =
      BeamFnApi.MonitoringInfosMetadataResponse.newBuilder();
    response.putAllMonitoringInfo(ShortIdCache.getShortIdCache().getInfos(
        request.getMonitoringInfos().getMonitoringInfoIdList()));
    LOG.info("ajamato monitoringInfoMetadata response " + response.toString());
    return BeamFnApi.InstructionResponse.newBuilder().setMonitoringInfos(response);
  }

  public BeamFnApi.InstructionResponse.Builder harnessMonitoringInfos(
      BeamFnApi.InstructionRequest request) {
    LOG.info("ajamato harnessMonitoringInfos0 request " + request.toString());
    BeamFnApi.HarnessMonitoringInfosResponse.Builder response =
        BeamFnApi.HarnessMonitoringInfosResponse.newBuilder();

    MetricsContainer container = MetricsEnvironment.getProcessWideContainer();
    if (container != null) {
      LOG.info("ajamato harnessMonitoringInfos1 hasContainer " + container);
      for (MetricsApi.MonitoringInfo info : container.getMonitoringInfos()) {
        LOG.info("ajamato harnessMonitoringInfos2 addMonitoringData ");
        response.putMonitoringData(
          ShortIdCache.getShortIdCache().getShortId(info), info.getPayload());
      }
    } else {
      LOG.info("ajamato harnessMonitoringInfos1 DOES NOT HAVE Container!");
    }
    LOG.info("ajamato harnessMonitoringInfos3 response " + response.toString());
    return BeamFnApi.InstructionResponse.newBuilder().setHarnessMonitoringInfos(response);
  }
}
