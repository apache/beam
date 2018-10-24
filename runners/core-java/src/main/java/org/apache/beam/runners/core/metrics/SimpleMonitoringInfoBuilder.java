package org.apache.beam.runners.core.metrics;


import java.time.Instant;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.BeamUrns;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoTypeUrns;


public class SimpleMonitoringInfoBuilder {

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

  private MonitoringInfo.Builder builder;

  public SimpleMonitoringInfoBuilder() {
    this.builder = MonitoringInfo.newBuilder();
  }

  public void setUrn(String urn) {
    this.builder.setUrn(urn);
  }

  public void setUrnForUserMetric(String namespace, String name) {
    this.builder.setUrn(userMetricUrn(namespace, name));
  }

  public void setTimestampToNow() {
    Instant time = Instant.now();
    this.builder.getTimestampBuilder()
        .setSeconds(time.getEpochSecond())
        .setNanos(time.getNano());
  }

  public void setInt64Value(long value) {
    this.builder.getMetricBuilder().getCounterDataBuilder().setInt64Value(value);
    this.builder.setType(SUM_INT64_TYPE);
  }

  public void setPTransformLabel(String ptransform) {
    this.builder.putLabels("PTRANSFORM", ptransform);
  }

  public MonitoringInfo build() {
    return this.builder.build();
  } // TODO add validation

}
