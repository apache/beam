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

import com.google.auto.value.AutoValue;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** Simple POJO representing a filter for querying metrics. */
@AutoValue
public abstract class MetricsFilter {

  public Set<String> steps() {
    return immutableSteps();
  }

  public Set<MetricNameFilter> names() {
    return immutableNames();
  }

  protected abstract ImmutableSet<String> immutableSteps();

  protected abstract ImmutableSet<MetricNameFilter> immutableNames();

  public static Builder builder() {
    return new AutoValue_MetricsFilter.Builder();
  }

  /** Builder for creating a {@link MetricsFilter}. */
  @AutoValue.Builder
  public abstract static class Builder {

    protected abstract ImmutableSet.Builder<MetricNameFilter> immutableNamesBuilder();

    protected abstract ImmutableSet.Builder<String> immutableStepsBuilder();

    /**
     * Add a {@link MetricNameFilter}.
     *
     * <p>If no name filters are specified then all metric names will be included
     *
     * <p>If one or more name filters are specified, then only metrics that match one or more of the
     * filters will be included.
     */
    public Builder addNameFilter(MetricNameFilter nameFilter) {
      immutableNamesBuilder().add(nameFilter);
      return this;
    }

    /**
     * Add a step filter.
     *
     * <p>If no steps are specified then metrics will be included for all steps.
     *
     * <p>If one or more steps are specified, then metrics will be included if they are part of any
     * of the specified steps.
     *
     * <p>The step names of metrics are identified as a path within the pipeline. So for example, a
     * transform that is applied with the name "bar" in a composite that was applied with the name
     * "foo" would have a step name of "foo/bar".
     *
     * <p>Step name filters may be either a full name (such as "foo/bar/baz") or a partial name such
     * as "foo", "bar" or "foo/bar". However, each component of the step name must be completely
     * matched, so the filter "foo" will not match a step name such as "fool/bar/foot"
     *
     * <p>TODO(https://github.com/apache/beam/issues/20919): Beam does not guarantee a specific
     * format for step IDs hence we should not assume a "foo/bar/baz" format here.
     */
    public Builder addStep(String step) {
      immutableStepsBuilder().add(step);
      return this;
    }

    public abstract MetricsFilter build();
  }
}
