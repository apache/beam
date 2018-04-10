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

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Methods for interacting with the metrics of a pipeline that has been executed. Accessed via
 * {@link PipelineResult#metrics()}.
 */
@Experimental(Kind.METRICS)
public abstract class MetricResults {
  /**
   * Query for all metric values that match a given filter.
   *
   * <p>The {@code filter} may filter based on the namespace and/or name of the metric, as well as
   * the step that reported the metric.
   *
   * <p>For each type of metric, the result contains an iterable of all metrics of that type that
   * matched the filter. Each {@link MetricResult} includes the name of the metric, the step in
   * which it was reported and the {@link MetricResult#getCommitted} and
   * {@link MetricResult#getAttempted} values.
   *
   * <p>Note that runners differ in their support for committed and attempted values.
   *
   * <p>Example: Querying the metrics reported from the {@code SomeDoFn} example in {@link Metrics}
   * could be done as follows:
   * <pre>{@code
   * Pipeline p = ...;
   * p.apply("create1", Create.of("hello")).apply("myStepName1", ParDo.of(new SomeDoFn()));
   * p.apply("create2", Create.of("world")).apply("myStepName2", ParDo.of(new SomeDoFn()));
   * PipelineResult result = p.run();
   * MetricResults metrics = result.metrics();
   * MetricQueryResults metricResults = metrics.queryMetrics(new MetricsFilter.Builder()
   *     .addNameFilter("my-counter")
   *     .addStepFilter("myStepName1").addStepFilter("myStepName2")
   *     .build());
   * Iterable<MetricResult<Long>> counters = metricResults.counters();
   * // counters should contain the value of my-counter reported from each of the ParDo
   * // applications.
   * }</pre>
   */
  public abstract MetricQueryResults queryMetrics(MetricsFilter filter);
}
