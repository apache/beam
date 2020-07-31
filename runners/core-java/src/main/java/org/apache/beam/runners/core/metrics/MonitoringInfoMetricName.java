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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@code MetricKey} based on a MonitoringInfo's URN and label to represent the
 * key instead of only a name+namespace. This is useful when defining system defined metrics with a
 * specific urn via a {@code CounterContainer}.
 */
public class MonitoringInfoMetricName extends MetricName {

  private String urn;
  private Map<String, String> labels = new HashMap<String, String>();

  private MonitoringInfoMetricName(String urn, Map<String, String> labels) {
    checkArgument(!Strings.isNullOrEmpty(urn), "MonitoringInfoMetricName urn must be non-empty");
    checkArgument(labels != null, "MonitoringInfoMetricName labels must be non-null");
    // TODO(ajamato): Move SimpleMonitoringInfoBuilder to :runners:core-construction-java
    // and ensure all necessary labels are set for the specific URN.
    this.urn = urn;
    for (Entry<String, String> entry : labels.entrySet()) {
      this.labels.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String getNamespace() {
    if (labels.containsKey(MonitoringInfoConstants.Labels.NAMESPACE)) {
      // User-generated metric
      return labels.getOrDefault(MonitoringInfoConstants.Labels.NAMESPACE, null);
    } else if (labels.containsKey(MonitoringInfoConstants.Labels.PCOLLECTION)) {
      // System-generated metric
      return labels.getOrDefault(MonitoringInfoConstants.Labels.PCOLLECTION, null);
    } else if (labels.containsKey(MonitoringInfoConstants.Labels.PTRANSFORM)) {
      // System-generated metric
      return labels.getOrDefault(MonitoringInfoConstants.Labels.PTRANSFORM, null);
    } else {
      return urn.split(":", 2)[0];
    }
  }

  @Override
  public String getName() {
    if (labels.containsKey(MonitoringInfoConstants.Labels.NAME)) {
      return labels.getOrDefault(MonitoringInfoConstants.Labels.NAME, null);
    } else {
      return urn.split(":", 2)[1];
    }
  }

  /** @return the urn of this MonitoringInfo metric. */
  public String getUrn() {
    return this.urn;
  }

  /** @return The labels associated with this MonitoringInfo. */
  public Map<String, String> getLabels() {
    return this.labels;
  }

  public static MonitoringInfoMetricName of(MetricsApi.MonitoringInfo mi) {
    return new MonitoringInfoMetricName(mi.getUrn(), mi.getLabelsMap());
  }

  public static MonitoringInfoMetricName named(String urn, HashMap<String, String> labels) {
    return new MonitoringInfoMetricName(urn, labels);
  }

  @Override
  public int hashCode() {
    // Don't include name and namespace, since they are lazily set.
    return Objects.hash(urn, labels);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    // If the object is compared with itself then return true
    if (o == this) {
      return true;
    }
    if (!(o instanceof MonitoringInfoMetricName)) {
      return false;
    }
    MonitoringInfoMetricName other = (MonitoringInfoMetricName) o;
    return this.urn.equals(other.urn) && this.labels.equals(other.labels);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.urn.toString());
    builder.append(" ");
    builder.append(this.labels.toString());
    return builder.toString();
  }
}
