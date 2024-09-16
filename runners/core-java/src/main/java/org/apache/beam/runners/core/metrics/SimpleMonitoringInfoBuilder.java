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

import static org.apache.beam.model.pipeline.v1.MetricsApi.monitoringInfoSpec;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleCounter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleDistribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Histogram;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeStringSet;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpec;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.sdk.util.HistogramData;
import org.checkerframework.checker.nullness.qual.Nullable;

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
 * builder.setPTransformLabel("myTransform");
 * builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myTransform"); MonitoringInfo mi =
 * builder.build();
 */
public class SimpleMonitoringInfoBuilder {
  private static final SpecMonitoringInfoValidator VALIDATOR = new SpecMonitoringInfoValidator();
  private final boolean validateAndDropInvalid;

  private static final Map<String, MonitoringInfoSpec> KNOWN_SPECS =
      new HashMap<String, MonitoringInfoSpec>();

  private MonitoringInfo.Builder builder;

  static {
    for (MonitoringInfoSpecs.Enum val : MonitoringInfoSpecs.Enum.values()) {
      // The enum iterator inserts an UNRECOGNIZED = -1 value which isn't explicitly added in
      // the proto files.
      if (!val.name().equals("UNRECOGNIZED")) {
        MonitoringInfoSpec spec =
            val.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec);
        KNOWN_SPECS.put(spec.getUrn(), spec);
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
   * Sets the type of the MonitoringInfo.
   *
   * @param type The type of the MonitoringInfo
   */
  public SimpleMonitoringInfoBuilder setType(String type) {
    this.builder.setType(type);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link MonitoringInfoConstants.TypeUrns#SUM_INT64_TYPE}.
   */
  public SimpleMonitoringInfoBuilder setInt64SumValue(long value) {
    this.builder.setPayload(encodeInt64Counter(value));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE);
    return this;
  }

  public SimpleMonitoringInfoBuilder setDoubleSumValue(double value) {
    this.builder.setPayload(encodeDoubleCounter(value));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.SUM_DOUBLE_TYPE);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link
   * MonitoringInfoConstants.TypeUrns#LATEST_INT64_TYPE}.
   */
  public SimpleMonitoringInfoBuilder setInt64LatestValue(GaugeData data) {
    checkArgument(GaugeData.empty() != data, "Cannot encode empty gauge data");
    this.builder.setPayload(encodeInt64Gauge(data));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link
   * MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}.
   */
  public SimpleMonitoringInfoBuilder setInt64DistributionValue(DistributionData data) {
    this.builder.setPayload(encodeInt64Distribution(data));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link
   * MonitoringInfoConstants.TypeUrns#PER_WORKER_HISTOGRAM}.
   */
  public SimpleMonitoringInfoBuilder setInt64HistogramValue(HistogramData data) {
    this.builder.setPayload(encodeInt64Histogram(data));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.PER_WORKER_HISTOGRAM_TYPE);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link
   * MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}.
   */
  public SimpleMonitoringInfoBuilder setDoubleDistributionValue(
      long count, double sum, double min, double max) {
    this.builder.setPayload(encodeDoubleDistribution(count, sum, min, max));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.DISTRIBUTION_DOUBLE_TYPE);
    return this;
  }

  /**
   * Encodes the value and sets the type to {@link
   * MonitoringInfoConstants.TypeUrns#SET_STRING_TYPE}.
   */
  public SimpleMonitoringInfoBuilder setStringSetValue(StringSetData value) {
    this.builder.setPayload(encodeStringSet(value));
    this.builder.setType(MonitoringInfoConstants.TypeUrns.SET_STRING_TYPE);
    return this;
  }

  /** Sets the MonitoringInfo label to the given name and value. */
  public SimpleMonitoringInfoBuilder setLabel(String labelName, String labelValue) {
    this.builder.putLabels(labelName, labelValue);
    return this;
  }

  /** Adds all the labels to the MonitoringInfo overwriting any duplicated keys. */
  public SimpleMonitoringInfoBuilder setLabels(Map<String, String> labels) {
    this.builder.putAllLabels(labels);
    return this;
  }

  public void clear() {
    this.builder = MonitoringInfo.newBuilder();
  }
  /** Clear the builder and merge from the provided monitoringInfo. */
  public void merge(MonitoringInfo monitoringInfo) {
    this.builder.mergeFrom(monitoringInfo);
  }

  /**
   * Builds the provided MonitoringInfo. Returns null if validateAndDropInvalid set and fields do
   * not match respecting MonitoringInfoSpec based on urn.
   */
  public @Nullable MonitoringInfo build() {
    final MonitoringInfo result = this.builder.build();
    if (validateAndDropInvalid && VALIDATOR.validate(result).isPresent()) {
      return null;
    }
    return result;
  }
}
