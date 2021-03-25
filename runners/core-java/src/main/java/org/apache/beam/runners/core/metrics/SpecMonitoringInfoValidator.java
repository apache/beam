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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpec;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;

/** Class implements validation of MonitoringInfos against MonitoringInfoSpecs. */
public class SpecMonitoringInfoValidator {
  protected final MonitoringInfoSpec[] specs;

  public SpecMonitoringInfoValidator() {
    specs =
        Arrays.stream(MonitoringInfoSpecs.Enum.values())
            // Filtering default value for "unknown" Enums. Coming from proto implementation.
            .filter(x -> !x.name().equals("UNRECOGNIZED"))
            .map(x -> x.getValueDescriptor().getOptions().getExtension(monitoringInfoSpec))
            .toArray(size -> new MonitoringInfoSpec[size]);
  }

  /**
   * Validates provided {link MonitoringInfo} against relevant {link MonitoringInfoSpecs} if
   * present.
   *
   * @return error string if validation fails.
   */
  public Optional<String> validate(MonitoringInfo monitoringInfo) {
    MonitoringInfoSpec spec = null;

    if (monitoringInfo.getUrn().isEmpty() || monitoringInfo.getType().isEmpty()) {
      return Optional.of(
          String.format(
              "MonitoringInfo requires both urn %s and type %s to be specified.",
              monitoringInfo.getUrn(), monitoringInfo.getType()));
    }

    for (MonitoringInfoSpec specIterator : specs) {
      if (monitoringInfo.getUrn().equals(specIterator.getUrn())
          && monitoringInfo.getType().equals(specIterator.getType())) {
        spec = specIterator;
        break;
      }
    }

    // Skip checking unknown MonitoringInfos
    if (spec == null) {
      return Optional.empty();
    }

    // TODO(ajamato): Tighten this restriction to use set equality, to catch unused
    Set<String> requiredLabels = new HashSet<>(spec.getRequiredLabelsList());
    if (!monitoringInfo.getLabelsMap().keySet().containsAll(requiredLabels)) {
      return Optional.of(
          String.format(
              "MonitoringInfo with urn: %s should have labels: %s, actual: %s",
              monitoringInfo.getUrn(), requiredLabels, monitoringInfo.getLabelsMap()));
    }

    return Optional.empty();
  }
}
