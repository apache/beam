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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Standard collection of metrics used to record source and sinks information for lineage tracking.
 */
public class Lineage {

  public static final String LINEAGE_NAMESPACE = "lineage";
  private static final Lineage SOURCES = new Lineage(Type.SOURCE);
  private static final Lineage SINKS = new Lineage(Type.SINK);
  // Reserved characters are backtick, colon, whitespace (space, \t, \n) and dot.
  private static final Pattern RESERVED_CHARS = Pattern.compile("[:\\s.`]");

  private final BoundedTrie metric;

  private Lineage(Type type) {
    this.metric = Metrics.boundedTrie(LINEAGE_NAMESPACE, type.toString());
  }

  /** {@link Lineage} representing sources and optionally side inputs. */
  public static Lineage getSources() {
    return SOURCES;
  }

  /** {@link Lineage} representing sinks. */
  public static Lineage getSinks() {
    return SINKS;
  }

  @VisibleForTesting
  static Iterator<String> getFQNParts(
      String system,
      @Nullable String subtype,
      List<String> segments,
      @Nullable String lastSegmentSep) {

    List<String> parts = new ArrayList<>();
    parts.add(system + ":");
    if (subtype != null) {
      parts.add(subtype + ":");
    }
    if (segments != null && segments.size() > 0) {
      for (int i = 0; i < segments.size() - 1; i++) {
        parts.add(wrapSegment(segments.get(i)) + ".");
      }
      if (lastSegmentSep != null) {
        List<String> subSegments =
            Splitter.onPattern(lastSegmentSep)
                .splitToList(wrapSegment(segments.get(segments.size() - 1)));
        for (int i = 0; i < subSegments.size() - 1; i++) {
          parts.add(subSegments.get(i) + lastSegmentSep);
        }
        parts.add(subSegments.get(subSegments.size() - 1));
      } else {
        parts.add(segments.get(segments.size() - 1));
      }
    }
    return parts.iterator();
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage. Segments will be processed via {@link
   * #getFQNParts}.
   */
  public void add(
      String system,
      @Nullable String subtype,
      Iterable<String> segments,
      @Nullable String lastSegmentSep) {
    List<String> result = new ArrayList<String>();
    segments.forEach(result::add);

    add(getFQNParts(system, subtype, result, lastSegmentSep));
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage. Segments will be processed via {@link
   * #getFQNParts}.
   */
  public void add(String system, Iterable<String> segments, @Nullable String lastSegmentSep) {
    add(system, null, segments, lastSegmentSep);
  }

  /**
   * Add a FQN (fully-qualified name) to Lineage. Segments will be processed via {@link
   * #getFQNParts}.
   */
  public void add(String system, Iterable<String> segments) {
    add(system, segments, null);
  }

  /**
   * Adds the given fqn as lineage.
   *
   * @param rollupSegments should be an iterable of strings whose concatenation is a valid <a
   *     href="https://cloud.google.com/data-catalog/docs/fully-qualified-names">Dataplex FQN </a>
   *     which is already escaped.
   *     <p>In particular, this means they will often have trailing delimiters.
   */
  public void add(Iterator<String> rollupSegments) {
    List<String> segments = new ArrayList<>();
    rollupSegments.forEachRemaining(segments::add);
    this.metric.add(segments);
  }

  /**
   * Query {@link BoundedTrie} metrics from {@link MetricResults}.
   *
   * @param results FQNs from the result.
   * @param type sources or sinks.
   * @param truncatedMarker the marker to use to represent truncated FQNs.
   * @return A flat representation of all FQNs. If the FQN was truncated then it has a trailing
   *     truncatedMarker.
   */
  public static Set<String> query(MetricResults results, Type type, String truncatedMarker) {
    MetricsFilter filter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(LINEAGE_NAMESPACE, type.toString()))
            .build();
    Set<String> result = new HashSet<>();
    truncatedMarker = truncatedMarker == null ? "*" : truncatedMarker;
    for (MetricResult<BoundedTrieResult> metrics : results.queryMetrics(filter).getBoundedTries()) {
      try {
        for (List<String> fqn : metrics.getCommitted().getResult()) {
          String end = Boolean.parseBoolean(fqn.get(fqn.size() - 1)) ? truncatedMarker : "";
          result.add(String.join("", fqn.subList(0, fqn.size() - 1)) + end);
        }
      } catch (UnsupportedOperationException unused) {
        // MetricsResult.getCommitted throws this exception when runner support missing, just skip.
      }
      for (List<String> fqn : metrics.getAttempted().getResult()) {
        String end = Boolean.parseBoolean(fqn.get(fqn.size() - 1)) ? truncatedMarker : "";
        result.add(String.join("", fqn.subList(0, fqn.size() - 1)) + end);
      }
    }
    return result;
  }

  /**
   * Query {@link BoundedTrie} metrics from {@link MetricResults}.
   *
   * @param results FQNs from the result
   * @param type sources or sinks
   * @return A flat representation of all FQNs. If the FQN was truncated then it has a trailing '*'.
   */
  public static Set<String> query(MetricResults results, Type type) {
    return query(results, type, "*");
  }

  /** Lineage metrics resource types. */
  public enum Type {
    SOURCE("sources_v2"),
    SINK("sinks_v2");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Wrap segment to valid segment name.
   *
   * <p>It escapes reserved characters
   *
   * <ul>
   *   <li>Reserved characters are backtick, colon, whitespace (space, \t, \n) and dot.
   *   <li>Only segments containing reserved characters must be escaped.
   *   <li>Segments cannot be escaped partially (i.e. “bigquery:com`.`google.test”).
   *   <li>Segments must be escaped using backticks (a.k.a. graves).
   *   <li>Backticks must be escaped using backtick (i.e. bigquery:`test``test`) and the segment
   *       itself must be escaped as well.
   * </ul>
   */
  @Internal
  public static String wrapSegment(String value) {
    value = value.replace("`", "``"); // Escape backticks
    // the escaped backticks will not throw this off since escaping will
    // happen if it contains ` in first place.
    if (RESERVED_CHARS.matcher(value).find()) {
      return String.format("`%s`", value);
    }
    return value;
  }
}
