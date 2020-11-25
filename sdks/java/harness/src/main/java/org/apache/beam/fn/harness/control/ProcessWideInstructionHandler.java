package org.apache.beam.fn.harness.control;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

// TODO can this be a static class?
public class ProcessWideInstructionHandler {

  public ProcessWideInstructionHandler() { }

  public BeamFnApi.InstructionResponse.Builder monitoringInfoMetadata(
      BeamFnApi.InstructionRequest request) {
    BeamFnApi.MonitoringInfosMetadataResponse.Builder response =
      BeamFnApi.MonitoringInfosMetadataResponse.newBuilder();
    response.putAllMonitoringInfo(ShortIdCache.getShortIdCache().getInfos(
        request.getMonitoringInfos().getMonitoringInfoIdList()));
    return BeamFnApi.InstructionResponse.newBuilder().setMonitoringInfos(response);
  }

  public BeamFnApi.InstructionResponse.Builder harnessMonitoringInfos(
      BeamFnApi.InstructionRequest request) {
    BeamFnApi.HarnessMonitoringInfosResponse.Builder response =
        BeamFnApi.HarnessMonitoringInfosResponse.newBuilder();

    MetricsContainer container = MetricsEnvironment.getProcessWideContainer();
    if (container != null) {
      for (MetricsApi.MonitoringInfo info : container.getMonitoringInfos()) {
        response.putMonitoringData(
          ShortIdCache.getShortIdCache().getShortId(info), info.getPayload());
      }
    }
    return BeamFnApi.InstructionResponse.newBuilder().setHarnessMonitoringInfos(response);
  }
}
