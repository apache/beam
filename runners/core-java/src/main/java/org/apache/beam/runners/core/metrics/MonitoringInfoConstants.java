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
import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo.MonitoringInfoLabels;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoTypeUrns;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

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
    public static final String USER_SUM_INT64 = extractUrn(MonitoringInfoSpecs.Enum.USER_SUM_INT64);
    public static final String USER_SUM_DOUBLE =
        extractUrn(MonitoringInfoSpecs.Enum.USER_SUM_DOUBLE);
    public static final String USER_DISTRIBUTION_INT64 =
        extractUrn(MonitoringInfoSpecs.Enum.USER_DISTRIBUTION_INT64);
    public static final String USER_DISTRIBUTION_DOUBLE =
        extractUrn(MonitoringInfoSpecs.Enum.USER_DISTRIBUTION_DOUBLE);
    public static final String SAMPLED_BYTE_SIZE =
        extractUrn(MonitoringInfoSpecs.Enum.SAMPLED_BYTE_SIZE);
    public static final String WORK_COMPLETED = extractUrn(MonitoringInfoSpecs.Enum.WORK_COMPLETED);
    public static final String WORK_REMAINING = extractUrn(MonitoringInfoSpecs.Enum.WORK_REMAINING);
    public static final String DATA_CHANNEL_READ_INDEX =
        extractUrn(MonitoringInfoSpecs.Enum.DATA_CHANNEL_READ_INDEX);
  }

  /** Standardised MonitoringInfo labels that can be utilized by runners. */
  public static final class Labels {
    public static final String PTRANSFORM = "PTRANSFORM";
    public static final String PCOLLECTION = "PCOLLECTION";
    public static final String WINDOWING_STRATEGY = "WINDOWING_STRATEGY";
    public static final String CODER = "CODER";
    public static final String ENVIRONMENT = "ENVIRONMENT";
    public static final String NAMESPACE = "NAMESPACE";
    public static final String NAME = "NAME";

    static {
      checkArgument(PTRANSFORM.equals(extractLabel(MonitoringInfoLabels.TRANSFORM)));
      checkArgument(PCOLLECTION.equals(extractLabel(MonitoringInfoLabels.PCOLLECTION)));
      checkArgument(
          WINDOWING_STRATEGY.equals(extractLabel(MonitoringInfoLabels.WINDOWING_STRATEGY)));
      checkArgument(CODER.equals(extractLabel(MonitoringInfoLabels.CODER)));
      checkArgument(ENVIRONMENT.equals(extractLabel(MonitoringInfoLabels.ENVIRONMENT)));
      checkArgument(NAMESPACE.equals(extractLabel(MonitoringInfoLabels.NAMESPACE)));
      checkArgument(NAME.equals(extractLabel(MonitoringInfoLabels.NAME)));
    }
  }

  /** MonitoringInfo type Urns. */
  public static final class TypeUrns {
    public static final String SUM_INT64_TYPE = "beam:metrics:sum_int64:v1";
    public static final String SUM_DOUBLE_TYPE = "beam:metrics:sum_double:v1";
    public static final String DISTRIBUTION_INT64_TYPE = "beam:metrics:distribution_int64:v1";
    public static final String DISTRIBUTION_DOUBLE_TYPE = "beam:metrics:distribution_double:v1";
    public static final String LATEST_INT64_TYPE = "beam:metrics:latest_int64:v1";
    public static final String LATEST_DOUBLE_TYPE = "beam:metrics:latest_double:v1";
    public static final String TOP_N_INT64_TYPE = "beam:metrics:top_n_int64:v1";
    public static final String TOP_N_DOUBLE_TYPE = "beam:metrics:top_n_double:v1";
    public static final String BOTTOM_N_INT64_TYPE = "beam:metrics:bottom_n_int64:v1";
    public static final String BOTTOM_N_DOUBLE_TYPE = "beam:metrics:bottom_n_double:v1";
    public static final String PROGRESS_TYPE = "beam:metrics:progress:v1";

    static {
      checkArgument(SUM_INT64_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE)));
      checkArgument(SUM_DOUBLE_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.SUM_DOUBLE_TYPE)));
      checkArgument(
          DISTRIBUTION_INT64_TYPE.equals(
              getUrn(MonitoringInfoTypeUrns.Enum.DISTRIBUTION_INT64_TYPE)));
      checkArgument(
          DISTRIBUTION_DOUBLE_TYPE.equals(
              getUrn(MonitoringInfoTypeUrns.Enum.DISTRIBUTION_DOUBLE_TYPE)));
      checkArgument(
          LATEST_INT64_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.LATEST_INT64_TYPE)));
      checkArgument(
          LATEST_DOUBLE_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.LATEST_DOUBLE_TYPE)));
      checkArgument(TOP_N_INT64_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.TOP_N_INT64_TYPE)));
      checkArgument(
          TOP_N_DOUBLE_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.TOP_N_DOUBLE_TYPE)));
      checkArgument(
          BOTTOM_N_INT64_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.BOTTOM_N_INT64_TYPE)));
      checkArgument(
          BOTTOM_N_DOUBLE_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.BOTTOM_N_DOUBLE_TYPE)));
      checkArgument(PROGRESS_TYPE.equals(getUrn(MonitoringInfoTypeUrns.Enum.PROGRESS_TYPE)));
    }
  }

  @VisibleForTesting
  static String extractUrn(MonitoringInfoSpecs.Enum value) {
    return value.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec).getUrn();
  }

  private static String extractLabel(MonitoringInfo.MonitoringInfoLabels value) {
    return value.getValueDescriptor().getOptions().getExtension(labelProps).getName();
  }
}
