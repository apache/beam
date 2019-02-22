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

import static org.apache.beam.model.pipeline.v1.MetricsApi.labelProps;
import static org.apache.beam.model.pipeline.v1.MetricsApi.monitoringInfoSpec;
import static org.apache.beam.runners.core.metrics.DistributionProtos.toProto;

import java.time.Instant;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.IntDistributionData;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo.MonitoringInfoLabels;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoLabelProps;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpec;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoTypeUrns;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoUrns;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplified building of MonitoringInfo fields, allows setting one field at a time with simpler
 * method calls, without needing to dive into the details of the nested protos.
 *
 * <p>There is no need to set the type field, by setting the appropriate value field: (i.e.
 * setInt64Value), the typeUrn field is automatically set.
 *
 * <p>Additionally, if validateAndDropInvalid is set to true in the ctor, then MonitoringInfos will
 * be returned as null when build() is called if any fields are not properly set. This is based on
 * comparing the fields which are set to the MonitoringInfoSpec in beam_fn_api.proto.
 *
 * <p>Example Usage (ElementCount counter):
 *
 * <p>SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
 * builder.setUrn(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN); builder.setInt64Value(1);
 * builder.setPTransformLabel("myTransform"); builder.setPCollectionLabel("myPcollection");
 * MonitoringInfo mi = builder.build();
 *
 * <p>Example Usage (ElementCount counter):
 *
 * <p>SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
 * builder.setUrn(SimpleMonitoringInfoBuilder.setUrnForUserMetric("myNamespace", "myName"));
 * builder.setInt64Value(1); MonitoringInfo mi = builder.build();
 */
public class SimpleMonitoringInfoBuilder {
  public static final String ELEMENT_COUNT_URN =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.ELEMENT_COUNT);
  public static final String START_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.START_BUNDLE_MSECS);
  public static final String PROCESS_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.PROCESS_BUNDLE_MSECS);
  public static final String FINISH_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.FINISH_BUNDLE_MSECS);
  public static final String USER_COUNTER_URN_PREFIX =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.USER_COUNTER_URN_PREFIX);
  public static final String SUM_INT64_TYPE_URN =
      BeamUrns.getUrn(MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);
  public static final String DISTRIBUTION_INT64_TYPE_URN =
      BeamUrns.getUrn(MonitoringInfoTypeUrns.Enum.DISTRIBUTION_INT64_TYPE);
  public static final String LATEST_INT64_TYPE_URN =
      BeamUrns.getUrn(MonitoringInfoTypeUrns.Enum.LATEST_INT64_TYPE);

  private static final HashMap<String, MonitoringInfoSpec> specs =
      new HashMap<String, MonitoringInfoSpec>();

  public static final String PCOLLECTION_LABEL = getLabelString(MonitoringInfoLabels.PCOLLECTION);
  public static final String PTRANSFORM_LABEL = getLabelString(MonitoringInfoLabels.TRANSFORM);

  private final boolean validateAndDropInvalid;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleMonitoringInfoBuilder.class);

  private MonitoringInfo.Builder builder;

  private SpecMonitoringInfoValidator validator = new SpecMonitoringInfoValidator();

  static {
    for (MonitoringInfoSpecs.Enum val : MonitoringInfoSpecs.Enum.values()) {
      // The enum iterator inserts an UNRECOGNIZED = -1 value which isn't explicitly added in
      // the proto files.
      if (!val.name().equals("UNRECOGNIZED")) {
        MonitoringInfoSpec spec =
            val.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec);
        SimpleMonitoringInfoBuilder.specs.put(spec.getUrn(), spec);
      }
    }
  }

  /** Returns the label string constant defined in the MonitoringInfoLabel enum proto. */
  private static String getLabelString(MonitoringInfoLabels label) {
    MonitoringInfoLabelProps props =
        label.getValueDescriptor().getOptions().getExtension(labelProps);
    return props.getName();
  }

  public SimpleMonitoringInfoBuilder() {
    this(true);
  }

  public SimpleMonitoringInfoBuilder(boolean validateAndDropInvalid) {
    this.builder = MonitoringInfo.newBuilder();
    this.validateAndDropInvalid = validateAndDropInvalid;
  }

  /** @return The metric URN for a user metric, with a proper URN prefix. */
  public static String userMetricUrn(String metricNamespace, String metricName) {
    String fixedMetricNamespace = metricNamespace.replace(':', '_');
    String fixedMetricName = metricName.replace(':', '_');
    StringBuilder sb = new StringBuilder();
    sb.append(USER_COUNTER_URN_PREFIX);
    sb.append(fixedMetricNamespace);
    sb.append(':');
    sb.append(fixedMetricName);
    return sb.toString();
  }

  /**
   * Sets the urn of the MonitoringInfo.
   *
   * @param urn The urn of the MonitoringInfo
   */
  public SimpleMonitoringInfoBuilder setUrn(String urn) {
    this.builder.setUrn(urn);
    return this;
  }

  public SimpleMonitoringInfoBuilder setLabelsAndUrnFrom(MetricKey key) {
    MetricName metricName = key.metricName();
    if (metricName instanceof MonitoringInfoMetricName) {
      MonitoringInfoMetricName name = (MonitoringInfoMetricName) metricName;
      builder.setUrn(name.getUrn()).putAllLabels(name.getLabels());
    } else {
      setUrnForUserMetric(metricName.getNamespace(), metricName.getName());
      String ptransform = key.stepName();
      if (ptransform != null) {
        setPTransformLabel(ptransform);
      } else {
        LOG.warn("User metric {} without step name set", metricName);
      }
    }
    return this;
  }

  /**
   * Sets the urn of the MonitoringInfo to a proper user metric URN for the given params.
   *
   * @param namespace
   * @param name
   */
  public SimpleMonitoringInfoBuilder setUrnForUserMetric(String namespace, String name) {
    this.builder.setUrn(userMetricUrn(namespace, name));
    return this;
  }

  /** Sets the timestamp of the MonitoringInfo to the specified time. */
  public SimpleMonitoringInfoBuilder setTimestamp(Instant instant) {
    this.builder.setTimestamp(
        Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()));
    return this;
  }

  /** Sets the timestamp of the MonitoringInfo to the current time. */
  public SimpleMonitoringInfoBuilder setTimestampToNow() {
    return setTimestamp(Instant.now());
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropriate type URN. */
  public SimpleMonitoringInfoBuilder setInt64Value(long value) {
    this.setInt64TypeUrn().builder.getMetricBuilder().getCounterDataBuilder().setInt64Value(value);
    return this;
  }

  /** Sets the the appropriate type URN for sum int64 counters. */
  public SimpleMonitoringInfoBuilder setInt64TypeUrn() {
    this.builder.setType(SUM_INT64_TYPE_URN);
    return this;
  }

  public SimpleMonitoringInfoBuilder setIntDistributionValue(DistributionData value) {
    return setIntDistributionValue(value.extractResult());
  }

  public SimpleMonitoringInfoBuilder setIntDistributionValue(DistributionResult value) {
    return setIntDistributionValue(toProto(value));
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropraite type URN. */
  public SimpleMonitoringInfoBuilder setIntDistributionValue(IntDistributionData value) {
    this.builder
        .setType(DISTRIBUTION_INT64_TYPE_URN)
        .getMetricBuilder()
        .getDistributionDataBuilder()
        .setIntDistributionData(value);
    return this;
  }

  public SimpleMonitoringInfoBuilder setGaugeValue(GaugeData value) {
    return setGaugeValue(value.extractResult());
  }

  public SimpleMonitoringInfoBuilder setGaugeValue(GaugeResult value) {
    return setGaugeValue(value.getValue());
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropraite type URN. */
  public SimpleMonitoringInfoBuilder setGaugeValue(long value) {
    this.builder
        .setType(LATEST_INT64_TYPE_URN)
        .getMetricBuilder()
        .getCounterDataBuilder()
        .setInt64Value(value);
    return this;
  }

  /** Sets the PTRANSFORM MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPTransformLabel(String pTransform) {
    // TODO(ajamato): Add validation that it is a valid pTransform name in the bundle descriptor.
    return setLabel(PTRANSFORM_LABEL, pTransform);
  }

  /** Sets the PCOLLECTION MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPCollectionLabel(String pCollection) {
    return setLabel(PCOLLECTION_LABEL, pCollection);
  }

  /** Sets the MonitoringInfo label to the given name and value. */
  public SimpleMonitoringInfoBuilder setLabel(String labelName, String labelValue) {
    this.builder.putLabels(labelName, labelValue);
    return this;
  }

  /** Clear the builder and merge from the provided monitoringInfo. */
  public void clearAndMerge(MonitoringInfo monitoringInfo) {
    this.builder = MonitoringInfo.newBuilder();
    this.builder.mergeFrom(monitoringInfo);
  }

  /**
   * @return A copy of the MonitoringInfo with the timestamp cleared, to allow comparing two
   *     MonitoringInfos.
   */
  @VisibleForTesting
  public static MonitoringInfo clearTimestamp(MonitoringInfo input) {
    MonitoringInfo.Builder builder = MonitoringInfo.newBuilder();
    builder.mergeFrom(input);
    builder.clearTimestamp();
    return builder.build();
  }

  /**
   * Builds the provided MonitoringInfo. Returns null if validateAndDropInvalid set and fields do
   * not match respecting MonitoringInfoSpec based on urn.
   */
  @Nullable
  public MonitoringInfo build() {
    final MonitoringInfo result = this.builder.build();
    if (validateAndDropInvalid && this.validator.validate(result).isPresent()) {
      return null;
    }
    return result;
  }
}
