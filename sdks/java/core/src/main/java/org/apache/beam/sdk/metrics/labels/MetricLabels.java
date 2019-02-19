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
package org.apache.beam.sdk.metrics.labels;

import static java.util.stream.Collectors.joining;
import static org.apache.beam.sdk.metrics.MetricUrns.PCOLLECTION_LABEL;
import static org.apache.beam.sdk.metrics.MetricUrns.PTRANSFORM_LABEL;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface around the allowed sets of labels that a metric may have.
 *
 * <p>Supported types:
 *
 * <ol>
 *   <li>{@link MetricLabels.PTransform PTRANSFORM only}
 *   <li>{@link MetricLabels.PCollection PCOLLECTION only}
 * </ol>
 */
public abstract class MetricLabels implements Serializable {
  /** Only instantiate this class as one of the concrete types below. */
  private MetricLabels() {}

  public abstract String key();

  public abstract String value();

  private Map<String, String> map;

  public Map<String, String> map() {
    if (map == null) {
      map = new HashMap<String, String>();
      map.put(key(), value());
    }
    return map;
  }

  @Nullable
  public abstract String ptransform();

  @Nullable
  public abstract String pcollection();

  @Override
  public String toString() {
    return toString("{", ":", "}");
  }

  public String toString(String openLabels, String labelKVDelimiter, String closeLabels) {
    return String.format("%s%s%s%s%s", openLabels, key(), labelKVDelimiter, value(), closeLabels);
  }

  public static MetricLabels ptransform(String ptransform) {
    return PTransform.of(ptransform);
  }

  public static MetricLabels pcollection(String pcollection) {
    return PCollection.of(pcollection);
  }

  public static MetricLabels create(Map<String, String> labels) {
    String ptransform = labels.get(PTRANSFORM_LABEL);
    if (ptransform != null) {
      return ptransform(ptransform);
    }
    String pcollection = labels.get(PCOLLECTION_LABEL);
    if (pcollection != null) {
      return pcollection(pcollection);
    }
    throw new IllegalStateException(
        "Can't create MetricLabels from MonitoringInfo labels: "
            + labels.entrySet().stream()
                .map(entry -> String.format("%s: %s", entry.getKey(), entry.getValue()))
                .collect(joining(", ")));
  }

  /** {@link MetricLabels} implementation for metrics scoped to only to a PTransform. */
  @AutoValue
  public abstract static class PTransform extends MetricLabels {

    public abstract String name();

    public static PTransform of(String name) {
      return new AutoValue_MetricLabels_PTransform(name);
    }

    @Nullable
    @Override
    public String ptransform() {
      return name();
    }

    @Nullable
    @Override
    public String pcollection() {
      return null;
    }

    @Override
    public String key() {
      return PTRANSFORM_LABEL;
    }

    @Override
    public String value() {
      return name();
    }
  }

  /** {@link MetricLabels} implementation for metrics scoped to only to a PCollection. */
  @AutoValue
  public abstract static class PCollection extends MetricLabels {

    public abstract String name();

    public static PCollection of(String name) {
      return new AutoValue_MetricLabels_PCollection(name);
    }

    @Nullable
    @Override
    public String ptransform() {
      return null;
    }

    @Nullable
    @Override
    public String pcollection() {
      return name();
    }

    @Override
    public String key() {
      return PCOLLECTION_LABEL;
    }

    @Override
    public String value() {
      return name();
    }
  }
}
