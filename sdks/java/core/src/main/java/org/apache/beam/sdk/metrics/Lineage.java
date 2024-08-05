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

import java.util.HashSet;
import java.util.Set;

/**
 * Standard collection of metrics used to record source and sinks information for lineage tracking.
 */
public class Lineage {
  public static final String LINEAGE_NAMESPACE = "lineage";
  private static final StringSet SOURCES =
      Metrics.stringSet(LINEAGE_NAMESPACE, Type.SOURCE.toString());
  private static final StringSet SINKS = Metrics.stringSet(LINEAGE_NAMESPACE, Type.SINK.toString());

  /** {@link StringSet} representing sources and optionally side inputs. */
  public static StringSet getSources() {
    return SOURCES;
  }

  /** {@link StringSet} representing sinks. */
  public static StringSet getSinks() {
    return SINKS;
  }

  /** {@link StringSet} representing {@link Type}. */
  public static StringSet get(Type type) {
    switch (type) {
      case SOURCE:
        return getSources();
      case SINK:
        return getSinks();
      default:
        throw new IllegalArgumentException(String.format("Unsupported Lineage type: %s", type));
    }
  }

  /** Query {@link StringSet} metrics from {@link MetricResults}. */
  public static Set<String> query(MetricResults results, Type type) {
    MetricsFilter filter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(LINEAGE_NAMESPACE, type.toString()))
            .build();
    Set<String> result = new HashSet<>();
    for (MetricResult<StringSetResult> metrics : results.queryMetrics(filter).getStringSets()) {
      result.addAll(metrics.getCommitted().getStringSet());
      result.addAll(metrics.getAttempted().getStringSet());
    }
    return result;
  }

  /** Lineage metrics resource types. */
  public enum Type {
    SOURCE("source"),
    SINK("sink");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
