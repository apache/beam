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

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo.MonitoringInfoLabels;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoTypeUrns;
import org.apache.beam.model.pipeline.v1.RunnerApi;

/** This static class fetches MonitoringInfo related values from metrics.proto. */
public final class MonitoringInfoConstants {

  /** Supported MonitoringInfo Urns. */
  public static final class Urns {
    public static final String ELEMENT_COUNT = extractUrn(MonitoringInfoSpecs.Enum.ELEMENT_COUNT);
    public static final String START_BUNDLE_MSECS =
        extractUrn(MonitoringInfoSpecs.Enum.START_BUNDLE_MSECS);
    public static final String PROCESS_BUNDLE_MSECS =
        extractUrn(MonitoringInfoSpecs.Enum.PROCESS_BUNDLE_MSECS);
    public static final String FINISH_BUNDLE_MSECS =
        extractUrn(MonitoringInfoSpecs.Enum.FINISH_BUNDLE_MSECS);
    public static final String TOTAL_MSECS = extractUrn(MonitoringInfoSpecs.Enum.TOTAL_MSECS);
    public static final String USER_COUNTER = extractUrn(MonitoringInfoSpecs.Enum.USER_COUNTER);
    public static final String USER_DISTRIBUTION_COUNTER =
        extractUrn(MonitoringInfoSpecs.Enum.USER_DISTRIBUTION_COUNTER);
    public static final String SAMPLED_BYTE_SIZE =
        extractUrn(MonitoringInfoSpecs.Enum.SAMPLED_BYTE_SIZE);
  }

  /** Standardised MonitoringInfo labels that can be utilized by runners. */
  public static final class Labels {
    public static final String PTRANSFORM = extractLabel(MonitoringInfoLabels.TRANSFORM);
    public static final String PCOLLECTION = extractLabel(MonitoringInfoLabels.PCOLLECTION);
    public static final String WINDOWING_STRATEGY =
        extractLabel(MonitoringInfoLabels.WINDOWING_STRATEGY);
    public static final String CODER = extractLabel(MonitoringInfoLabels.CODER);
    public static final String ENVIRONMENT = extractLabel(MonitoringInfoLabels.ENVIRONMENT);
    public static final String NAMESPACE = extractLabel(MonitoringInfoLabels.NAMESPACE);
    public static final String NAME = extractLabel(MonitoringInfoLabels.NAME);
  }

  /** MonitoringInfo type Urns. */
  public static final class TypeUrns {
    public static final String SUM_INT64 = extractLabel(MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);
    public static final String DISTRIBUTION_INT64 =
        extractLabel(MonitoringInfoTypeUrns.Enum.DISTRIBUTION_INT64_TYPE);
    public static final String LATEST_INT64 =
        extractLabel(MonitoringInfoTypeUrns.Enum.LATEST_INT64_TYPE);
  }

  private static String extractUrn(MonitoringInfoSpecs.Enum value) {
    return value.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec).getUrn();
  }

  private static String extractLabel(MonitoringInfo.MonitoringInfoLabels value) {
    return value.getValueDescriptor().getOptions().getExtension(labelProps).getName();
  }

  private static String extractLabel(MonitoringInfoTypeUrns.Enum value) {
    return value.getValueDescriptor().getOptions().getExtension(RunnerApi.beamUrn);
  }
}
