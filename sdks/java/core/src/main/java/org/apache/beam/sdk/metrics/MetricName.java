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
package org.apache.beam.sdk.metrics;

import static org.apache.beam.sdk.metrics.MetricUrns.USER_COUNTER_URN_PREFIX;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Splitter;

/**
 * Wrapper for {@link org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo} URN.
 *
 * <p>"User" metrics (URN {@link MetricUrns#USER_COUNTER_URN_PREFIX}) are defined by a "namespace"
 * and "name", and are the most commonly dealt with by user code, so structured constructors and
 * accessors are provided in terms of those strings.
 *
 * <p>The {@link #namespace} allows grouping related metrics together and also prevents collisions
 * between multiple metrics with the same name.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricName implements Serializable {

  public abstract String urn();

  @Nullable private String name;

  @Nullable private String namespace;

  /** Parse the urn field into a name and namespace field. */
  private boolean parseUrn(boolean raise) {
    if (namespace != null && name != null) {
      return true;
    }

    String urn = urn();
    if (!urn.startsWith(USER_COUNTER_URN_PREFIX)) {
      if (raise) {
        throw new IllegalStateException(
            "Attempting to access namespace/name of a non-user metric: " + urn);
      }
      return false;
    }
    urn = urn.substring(USER_COUNTER_URN_PREFIX.length());

    List<String> pieces = Splitter.on(':').splitToList(urn);
    if (pieces.size() < 1) {
      throw new IllegalArgumentException("Invalid user-metric URN: " + urn);
    }
    namespace = String.join("", pieces.subList(0, pieces.size() - 1));
    name = pieces.get(pieces.size() - 1);

    return true;
  }

  public Boolean isUserMetric() {
    return parseUrn(false);
  }

  /** @return the parsed namespace from the user metric URN, otherwise throws. */
  public String getNamespace() {
    if (this.namespace == null) {
      parseUrn(true);
    }
    return this.namespace;
  }
  /** @return the parsed name from the user metric URN, otherwise throws. */
  public String getName() {
    if (this.name == null) {
      parseUrn(true);
    }
    return this.name;
  }

  @Override
  public String toString() {
    return toString(":");
  }

  public String toString(String delimiter) {
    if (isUserMetric()) {
      return String.join(delimiter, namespace, name);
    }
    return urn().replaceAll(":", delimiter);
  }

  public static MetricName of(String urn) {
    return new AutoValue_MetricName(urn);
  }

  public static MetricName named(String namespace, String name) {
    return new AutoValue_MetricName(MetricUrns.urn(namespace, name));
  }

  public static MetricName named(Class<?> namespace, String name) {
    return new AutoValue_MetricName(MetricUrns.urn(namespace.getName(), name));
  }
}
