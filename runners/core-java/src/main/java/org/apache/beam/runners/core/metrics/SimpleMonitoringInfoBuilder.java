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

import java.time.Instant;
import java.util.HashMap;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpec;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpecs;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoTypeUrns;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns;
import org.apache.beam.runners.core.construction.BeamUrns;
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
  public static final String USER_COUNTER_URN_PREFIX =
      BeamUrns.getUrn(MonitoringInfoUrns.Enum.USER_COUNTER_URN_PREFIX);
  public static final String SUM_INT64_TYPE_URN =
      BeamUrns.getUrn(MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);

  private static final HashMap<String, MonitoringInfoSpec> specs =
      new HashMap<String, MonitoringInfoSpec>();

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
            val.getValueDescriptor().getOptions().getExtension(BeamFnApi.monitoringInfoSpec);
        SimpleMonitoringInfoBuilder.specs.put(spec.getUrn(), spec);
      }
    }
  }

  public SimpleMonitoringInfoBuilder() {
    this(true);
  }

  public SimpleMonitoringInfoBuilder(boolean validateAndDropInvalid) {
    this.builder = MonitoringInfo.newBuilder();
    this.validateAndDropInvalid = validateAndDropInvalid;
  }

  /** @return The metric URN for a user metric, with a proper URN prefix. */
  private static String userMetricUrn(String metricNamespace, String metricName) {
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

  /** Sets the timestamp of the MonitoringInfo to the current time. */
  public SimpleMonitoringInfoBuilder setTimestampToNow() {
    Instant time = Instant.now();
    this.builder.getTimestampBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano());
    return this;
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropraite type URN. */
  public SimpleMonitoringInfoBuilder setInt64Value(long value) {
    this.builder.getMetricBuilder().getCounterDataBuilder().setInt64Value(value);
    this.builder.setType(SUM_INT64_TYPE_URN);
    return this;
  }

  /** Sets the PTRANSFORM MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPTransformLabel(String pTransform) {
    // TODO(ajamato): Add validation that it is a valid pTransform name in the bundle descriptor.
    setLabel("PTRANSFORM", pTransform);
    return this;
  }

  /** Sets the PCOLLECTION MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPCollectionLabel(String pCollection) {
    setLabel("PCOLLECTION", pCollection);
    return this;
  }

  /** Sets the MonitoringInfo label to the given name and value. */
  public SimpleMonitoringInfoBuilder setLabel(String labelName, String labelValue) {
    this.builder.putLabels(labelName, labelValue);
    return this;
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
