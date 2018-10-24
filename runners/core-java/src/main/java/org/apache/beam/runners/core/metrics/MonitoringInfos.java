package org.apache.beam.runners.core.metrics;


import org.apache.beam.runners.core.construction.BeamUrns;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoTypeUrns;

public class MonitoringInfos {

  public static final String ELEMENT_COUNT_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.ELEMENT_COUNT);
  public static final String START_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.START_BUNDLE_MSECS);
  public static final String PROCESS_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.PROCESS_BUNDLE_MSECS);
  public static final String FINISH_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.FINISH_BUNDLE_MSECS);
  public static final String TOTAL_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.TOTAL_MSECS);
  public static final String USER_COUNTER_URN_PREFIX = BeamUrns.getUrn(MonitoringInfoUrns.Enum.USER_COUNTER_URN_PREFIX);

  public static final String SUM_INT64_TYPE = BeamUrns.getUrn(
      MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);
  public static final String DISTRIBUTION_INT64_TYPE = BeamUrns.getUrn(
      MonitoringInfoTypeUrns.Enum.DISTRIBUTION_INT64_TYPE);
  public static final String LATEST_INT64_TYPE = BeamUrns.getUrn(
      MonitoringInfoTypeUrns.Enum.LATEST_INT64_TYPE);

  /**
   * @param namespace The namespace of the metric.
   * @param name The name of the metric.
   * @return The metric URN for a user metric, with a proper URN prefix.
   */
  private static String userMetricUrn(String namespace, String name) {
    StringBuilder sb = new StringBuilder();
    sb.append(USER_COUNTER_URN_PREFIX);
    sb.append(namespace);
    sb.append(':');
    sb.append(name);
    return sb.toString();
  }

  private void setUrn(String urn) {
    // Set the type
    // TODO assert another type hasn't been set.
  }

  private void setTimestampToNow() {
    // Set the type
    // TODO assert another type hasn't been set.
    seconds = int(timestamp_secs)
        nanos = int((timestamp_secs - seconds) * 10**9)
    return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)
  }

  private void setInt64Value() {
    // Set the type
    // TODO assert another type hasn't been set.
  }

  private void setPTransformLabel() {
    // Set the type
    // TODO assert another type hasn't been set.
  }

  private void setInt64Value() {
    // Set the type
    // TODO assert another type hasn't been set.
  }

  // Consider making a builder?
  // Set Int64 (Type is automatically set then).
  // Set URN.
  // Set Label Value.

}
