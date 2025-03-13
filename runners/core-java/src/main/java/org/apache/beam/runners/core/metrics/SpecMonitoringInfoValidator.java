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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpec;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** Class implements validation of MonitoringInfos against MonitoringInfoSpecs. */
public class SpecMonitoringInfoValidator {
  // URN -> Type -> MonitoringInfoSpec required labels
  private static final Map<String, Map<String, Set<String>>> REQUIRED_LABELS = new HashMap<>();

  static {
    for (MonitoringInfoSpecs.Enum enumSpec : MonitoringInfoSpecs.Enum.values()) {
      // The enum iterator inserts an UNRECOGNIZED = -1 value which isn't explicitly added in
      // the proto files.
      if (!enumSpec.name().equals("UNRECOGNIZED")) {
        MonitoringInfoSpec spec =
            enumSpec.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec);
        Map<String, Set<String>> typeToSpec =
            REQUIRED_LABELS.computeIfAbsent(spec.getUrn(), (String key) -> new HashMap<>());
        Preconditions.checkState(
            typeToSpec.put(spec.getType(), new HashSet<>(spec.getRequiredLabelsList())) == null,
            String.format(
                "Found duplicate specs for urn %s and type %s.", spec.getUrn(), spec.getType()));
      }
    }
  }

  /**
   * Validates provided {link MonitoringInfo} against relevant {link MonitoringInfoSpecs} if
   * present.
   *
   * @return error string if validation fails.
   */
  public Optional<String> validate(MonitoringInfo monitoringInfo) {

    if (monitoringInfo.getUrn().isEmpty() || monitoringInfo.getType().isEmpty()) {
      return Optional.of(
          String.format(
              "MonitoringInfo requires both urn %s and type %s to be specified.",
              monitoringInfo.getUrn(), monitoringInfo.getType()));
    }

    // Skip checking unknown MonitoringInfos
    Map<String, Set<String>> typeToRequiredLabels = REQUIRED_LABELS.get(monitoringInfo.getUrn());
    if (typeToRequiredLabels == null) {
      return Optional.empty();
    }
    Set<String> requiredLabels = typeToRequiredLabels.get(monitoringInfo.getType());
    if (requiredLabels == null) {
      return Optional.empty();
    }

    // TODO(ajamato): Tighten this restriction to use set equality, to catch unused
    if (!monitoringInfo.getLabelsMap().keySet().containsAll(requiredLabels)) {
      return Optional.of(
          String.format(
              "MonitoringInfo with urn: %s should have labels: %s, actual: %s",
              monitoringInfo.getUrn(), requiredLabels, monitoringInfo.getLabelsMap()));
    }

    return Optional.empty();
  }
}
