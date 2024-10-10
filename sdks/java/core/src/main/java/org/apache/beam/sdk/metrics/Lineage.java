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
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Standard collection of metrics used to record source and sinks information for lineage tracking.
 */
public class Lineage {

  public static final String LINEAGE_NAMESPACE = "lineage";
  private static final Lineage SOURCES = new Lineage(Type.SOURCE);
  private static final Lineage SINKS = new Lineage(Type.SINK);
  private static final Pattern RESERVED_CHARS = Pattern.compile("[:\\s.]");

  private final StringSet metric;

  private Lineage(Type type) {
    this.metric = Metrics.stringSet(LINEAGE_NAMESPACE, type.toString());
  }

  /** {@link Lineage} representing sources and optionally side inputs. */
  public static Lineage getSources() {
    return SOURCES;
  }

  /** {@link Lineage} representing sinks. */
  public static Lineage getSinks() {
    return SINKS;
  }

  /**
   * Wrap segment to valid segment name.
   *
   * <p>Specifically, If there are reserved chars (colon, whitespace, dot), escape with backtick. If
   * the segment is already wrapped, return the original.
   *
   * <p>This helper method is for internal and testing usage only.
   */
  @Internal
  public static String wrapSegment(String value) {
    if (value.startsWith("`") && value.endsWith("`")) {
      return value;
    }
    if (RESERVED_CHARS.matcher(value).find()) {
      return String.format("`%s`", value);
    }
    return value;
  }

  /**
   * Assemble fully qualified name (<a
   * href="https://cloud.google.com/data-catalog/docs/fully-qualified-names">FQN</a>). Format:
   *
   * <ul>
   *   <li>{@code system:segment1.segment2}
   *   <li>{@code system:subtype:segment1.segment2}
   *   <li>{@code system:`segment1.with.dots:clons`.segment2}
   * </ul>
   *
   * <p>This helper method is for internal and testing usage only.
   */
  @Internal
  public static String getFqName(
      String system, @Nullable String subtype, Iterable<String> segments) {
    StringBuilder builder = new StringBuilder(system);
    if (!Strings.isNullOrEmpty(subtype)) {
      builder.append(":").append(subtype);
    }
    int idx = 0;
    for (String segment : segments) {
      if (idx == 0) {
        builder.append(":");
      } else {
        builder.append(".");
      }
      builder.append(wrapSegment(segment));
      ++idx;
    }
    return builder.toString();
  }

  /**
   * Assemble the FQN of given system, and segments.
   *
   * <p>This helper method is for internal and testing usage only.
   */
  @Internal
  public static String getFqName(String system, Iterable<String> segments) {
    return getFqName(system, null, segments);
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage. Segments will be processed via {@link #getFqName}.
   */
  public void add(String system, @Nullable String subtype, Iterable<String> segments) {
    add(getFqName(system, subtype, segments));
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage. Segments will be processed via {@link #getFqName}.
   */
  public void add(String system, Iterable<String> segments) {
    add(system, null, segments);
  }

  /**
   * Adds the given details as Lineage. For asset level lineage the resource location should be
   * specified as Dataplex FQN https://cloud.google.com/data-catalog/docs/fully-qualified-names
   */
  public void add(String details) {
    metric.add(details);
  }

  /** Query {@link StringSet} metrics from {@link MetricResults}. */
  public static Set<String> query(MetricResults results, Type type) {
    MetricsFilter filter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(LINEAGE_NAMESPACE, type.toString()))
            .build();
    Set<String> result = new HashSet<>();
    for (MetricResult<StringSetResult> metrics : results.queryMetrics(filter).getStringSets()) {
      try {
        result.addAll(metrics.getCommitted().getStringSet());
      } catch (UnsupportedOperationException unused) {
        // MetricsResult.getCommitted throws this exception when runner support missing, just skip.
      }
      result.addAll(metrics.getAttempted().getStringSet());
    }
    return result;
  }

  /** Lineage metrics resource types. */
  public enum Type {
    SOURCE("sources"),
    SINK("sinks");

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
