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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.lineage.LineageRegistrar;
import org.apache.beam.sdk.metrics.Metrics.MetricsFlag;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard collection of metrics used to record source and sinks information for lineage tracking.
 */
public abstract class Lineage {
  public static final String LINEAGE_NAMESPACE = "lineage";
  private static final Logger LOG = LoggerFactory.getLogger(Lineage.class);
  private static final AtomicReference<Lineage> SOURCES = new AtomicReference<>();
  private static final AtomicReference<Lineage> SINKS = new AtomicReference<>();

  private static final AtomicReference<KV<Long, Integer>> LINEAGE_REVISION =
      new AtomicReference<>();

  // Reserved characters are backtick, colon, whitespace (space, \t, \n) and dot.
  private static final Pattern RESERVED_CHARS = Pattern.compile("[:\\s.`]");

  protected Lineage() {}

  @Internal
  public static void initialize(PipelineOptions options) {
    checkNotNull(options, "options cannot be null");
    long optionsId = options.getOptionsId();
    int nextRevision = options.revision();

    while (true) {
      KV<Long, Integer> currentRevision = LINEAGE_REVISION.get();

      if (currentRevision != null
          && currentRevision.getKey().equals(optionsId)
          && currentRevision.getValue() >= nextRevision) {
        LOG.debug(
            "Lineage already initialized with options ID {} revision {}, skipping",
            optionsId,
            currentRevision.getValue());
        return;
      }

      if (LINEAGE_REVISION.compareAndSet(currentRevision, KV.of(optionsId, nextRevision))) {
        Lineage sources = createLineage(options, Type.SOURCE);
        Lineage sinks = createLineage(options, Type.SINK);

        SOURCES.set(sources);
        SINKS.set(sinks);

        if (currentRevision == null) {
          LOG.info("Lineage initialized with options ID {} revision {}", optionsId, nextRevision);
        } else {
          LOG.info(
              "Lineage re-initialized from options ID {} to {} (revision {} -> {})",
              currentRevision.getKey(),
              optionsId,
              currentRevision.getValue(),
              nextRevision);
        }
        return;
      }
    }
  }

  private static Lineage createLineage(PipelineOptions options, Type type) {
    Set<LineageRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(LineageRegistrar.class, ReflectHelpers.findClassLoader())));

    for (LineageRegistrar registrar : registrars) {
      Lineage reporter = registrar.fromOptions(options, type);
      if (reporter != null) {
        LOG.info("Using {} for lineage type {}", reporter.getClass().getName(), type);
        return reporter;
      }
    }

    LOG.debug("Using default Metrics-based lineage for type {}", type);
    return new MetricsLineage(type);
  }

  /** Get {@link Lineage} representing sources and optionally side inputs. */
  public static Lineage getSources() {
    Lineage sources = SOURCES.get();
    if (sources == null) {
      initialize(PipelineOptionsFactory.create());
      sources = SOURCES.get();
    }
    return sources;
  }

  /** {@link Lineage} representing sinks. */
  public static Lineage getSinks() {
    Lineage sinks = SINKS.get();
    if (sinks == null) {
      initialize(PipelineOptionsFactory.create());
      sinks = SINKS.get();
    }
    return sinks;
  }

  @VisibleForTesting
  static Iterable<String> getFQNParts(
      String system,
      @Nullable String subtype,
      Iterable<String> segments,
      @Nullable String lastSegmentSep) {

    List<String> parts = new ArrayList<>();
    parts.add(system + ":");
    if (subtype != null) {
      parts.add(subtype + ":");
    }

    if (segments != null) {
      Iterator<String> iterator = segments.iterator();
      String previousSegment = null;
      while (iterator.hasNext()) {
        if (previousSegment != null) {
          parts.add(wrapSegment(previousSegment) + ".");
        }
        previousSegment = iterator.next();
      }

      if (previousSegment != null) {
        if (lastSegmentSep != null) {
          List<String> subSegments =
              Splitter.onPattern(lastSegmentSep).splitToList(wrapSegment(previousSegment));
          for (int i = 0; i < subSegments.size() - 1; i++) {
            parts.add(subSegments.get(i) + lastSegmentSep);
          }
          parts.add(subSegments.get(subSegments.size() - 1));
        } else {
          parts.add(wrapSegment(previousSegment));
        }
      }
    }
    return parts;
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
    add(getFQNParts(system, subtype, segments, lastSegmentSep));
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
  public abstract void add(Iterable<String> rollupSegments);

  /**
   * Query {@link BoundedTrie} metrics from {@link MetricResults}.
   *
   * @param results FQNs from the result.
   * @param type sources or sinks.
   * @param truncatedMarker the marker to use to represent truncated FQNs.
   * @return A flat representation of all FQNs. If the FQN was truncated then it has a trailing
   *     truncatedMarker.
   *     <p>NOTE: When using a custom LineageReporter plugin, this method will return empty results
   *     since lineage is not stored in Metrics.
   */
  public static Set<String> query(MetricResults results, Type type, String truncatedMarker) {
    MetricQueryResults lineageQueryResults = getLineageQueryResults(results, type);
    Set<String> result = new HashSet<>();
    truncatedMarker = truncatedMarker == null ? "*" : truncatedMarker;
    for (MetricResult<BoundedTrieResult> metrics : lineageQueryResults.getBoundedTries()) {
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
   *     <p>NOTE: When using a custom LineageReporter plugin, this method will return empty results
   *     since lineage is not stored in Metrics.
   */
  public static Set<String> query(MetricResults results, Type type) {
    if (MetricsFlag.lineageRollupEnabled()) {
      // If user accidentally end up specifying V1 type then override it with V2.
      if (type == Type.SOURCE) {
        type = Type.SOURCEV2;
      }
      if (type == Type.SINK) {
        type = Type.SINKV2;
      }
      return query(results, type, "*");
    } else {
      return queryLineageV1(results, type);
    }
  }

  /** Lineage metrics resource types. */
  public enum Type {
    // Used by StringSet to report lineage metrics
    SOURCE("sources"),
    SINK("sinks"),

    // Used by BoundedTrie to report lineage metrics
    SOURCEV2("sources_v2"),
    SINKV2("sinks_v2");

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
   * Query {@link StringSet} metrics from {@link MetricResults}. This method is kept as-is from
   * previous Beam release for removal once BoundedTrie is made default.
   */
  private static Set<String> queryLineageV1(MetricResults results, Type type) {
    MetricQueryResults lineageQueryResults = getLineageQueryResults(results, type);
    Set<String> result = new HashSet<>();
    for (MetricResult<StringSetResult> metrics : lineageQueryResults.getStringSets()) {
      try {
        result.addAll(metrics.getCommitted().getStringSet());
      } catch (UnsupportedOperationException unused) {
        // MetricsResult.getCommitted throws this exception when runner support missing, just skip.
      }
      result.addAll(metrics.getAttempted().getStringSet());
    }
    return result;
  }

  /** @return {@link MetricQueryResults} containing lineage metrics. */
  private static MetricQueryResults getLineageQueryResults(MetricResults results, Type type) {
    MetricsFilter filter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(LINEAGE_NAMESPACE, type.toString()))
            .build();
    return results.queryMetrics(filter);
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
