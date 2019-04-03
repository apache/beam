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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Strings;

/**
 * An implementation of {@code MetricKey} based on a MonitoringInfo's URN and label to represent the
 * key instead of only a name+namespace. This is useful when defining system defined metrics with a
 * specific urn via a {@code CounterContainer}.
 */
public class MonitoringInfoMetricName extends MetricName {

  private String urn;
  @Nullable private String name;
  @Nullable private String namespace;
  private HashMap<String, String> labels = new HashMap<String, String>();

  private MonitoringInfoMetricName(String urn, HashMap<String, String> labels) {
    checkArgument(!Strings.isNullOrEmpty(urn), "MonitoringInfoMetricName urn must be non-empty");
    checkArgument(labels != null, "MonitoringInfoMetricName labels must be non-null");
    // TODO(ajamato): Move SimpleMonitoringInfoBuilder to beam-runner-core-construction-java
    // and ensure all necessary labels are set for the specific URN.
    this.urn = urn;
    for (Entry<String, String> entry : labels.entrySet()) {
      this.labels.put(entry.getKey(), entry.getValue());
    }
  }

  /** Parse the urn field into a name and namespace field. */
  private void parseUrn() {
    if (this.urn.startsWith(MonitoringInfoConstants.Urns.USER_COUNTER_PREFIX)) {
      List<String> split = new ArrayList<String>(Arrays.asList(this.getUrn().split(":")));
      this.name = split.get(split.size() - 1);
      this.namespace = split.get(split.size() - 2);
    }
  }

  /** @return the parsed namespace from the user metric URN, otherwise null. */
  @Override
  public String getNamespace() {
    if (this.namespace == null) {
      parseUrn();
    }
    return this.namespace;
  }

  /** @return the parsed name from the user metric URN, otherwise null. */
  @Override
  public String getName() {
    if (this.name == null) {
      parseUrn();
    }
    return this.name;
  }

  /** @return the urn of this MonitoringInfo metric. */
  public String getUrn() {
    return this.urn;
  }

  /** @return The labels associated with this MonitoringInfo. */
  public HashMap<String, String> getLabels() {
    return this.labels;
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
  public boolean equals(Object o) {
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
