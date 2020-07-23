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

import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implements matching for metrics filters. Specifically, matching for metric name, namespace, and
 * step name.
 */
public class MetricFiltering {

  private MetricFiltering() {}

  /**
   * Matching logic is implemented here rather than in MetricsFilter because we would like
   * MetricsFilter to act as a "dumb" value-object, with the possibility of replacing it with a
   * Proto/JSON/etc. schema object.
   *
   * @param filter {@link MetricsFilter} with the matching information of an actual metric
   * @param key {@link MetricKey} with the information of a metric
   * @return whether the filter matches the key or not
   */
  public static boolean matches(MetricsFilter filter, MetricKey key) {
    if (filter == null) {
      return true;
    }

    @Nullable String stepName = key.stepName();
    if (stepName == null) {
      if (!filter.steps().isEmpty()) {
        // The filter specifies steps, but the metric is not associated with a step.
        return false;
      }
    } else if (!matchesScope(stepName, filter.steps())) {
      // The filter specifies steps that differ from the metric's step
      return false;
    }

    // The filter's steps match the metric's step.
    return matchesName(key.metricName(), filter.names());
  }

  /**
   * {@code subPathMatches(haystack, needle)} returns true if {@code needle} represents a path
   * within {@code haystack}. For example, "foo/bar" is in "a/foo/bar/b", but not "a/fool/bar/b" or
   * "a/foo/bart/b".
   */
  public static boolean subPathMatches(String haystack, String needle) {
    int location = haystack.indexOf(needle);
    int end = location + needle.length();
    if (location == -1) {
      return false; // needle not found
    } else if (location != 0 && haystack.charAt(location - 1) != '/') {
      return false; // the first entry in needle wasn't exactly matched
    } else if (end != haystack.length() && haystack.charAt(end) != '/') {
      return false; // the last entry in needle wasn't exactly matched
    } else {
      return true;
    }
  }

  /**
   * {@code matchesScope(actualScope, scopes)} returns true if the scope of a metric is matched by
   * any of the filters in {@code scopes}. A metric scope is a path of type "A/B/D". A path is
   * matched by a filter if the filter is equal to the path (e.g. "A/B/D", or if it represents a
   * subpath within it (e.g. "A/B" or "B/D", but not "A/D").
   *
   * <p>Per https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/util/Set.html "sets
   * contain ... at most one null element" / "Some set implementations have restrictions on the
   * elements that they may contain. For example, some implementations prohibit null elements".
   * Since sets cannot in general contain null it is not safe to check for membership of null, so
   * the caller must determine what to do with a null {@code actualScope}.
   */
  public static boolean matchesScope(String actualScope, Set<String> scopes) {
    if (scopes.isEmpty() || scopes.contains(actualScope)) {
      return true;
    }

    // If there is no perfect match, a stage name-level match is tried.
    // This is done by a substring search over the levels of the scope.
    // e.g. a scope "A/B/C/D" is matched by "A/B", but not by "A/C".
    for (String scope : scopes) {
      if (subPathMatches(actualScope, scope)) {
        return true;
      }
    }

    return false;
  }

  private static boolean matchesName(MetricName metricName, Set<MetricNameFilter> nameFilters) {
    if (nameFilters.isEmpty()) {
      return true;
    }
    for (MetricNameFilter nameFilter : nameFilters) {
      @Nullable String name = nameFilter.getName();
      if (((name == null) || name.equals(metricName.getName()))
          && Objects.equal(metricName.getNamespace(), nameFilter.getNamespace())) {
        return true;
      }
    }
    return false;
  }
}
