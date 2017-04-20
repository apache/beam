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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.auto.value.AutoValue;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link MetricResults} for the Dataflow Runner.
 */
class DataflowMetrics extends MetricResults {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowMetrics.class);
  /**
   * Client for the Dataflow service. This can be used to query the service
   * for information about the job.
   */
  private DataflowClient dataflowClient;

  /**
   * PipelineResult implementation for Dataflow Runner. It contains job state and id information.
   */
  private DataflowPipelineJob dataflowPipelineJob;

  /**
   * After the job has finished running, Metrics no longer will change, so their results are
   * cached here.
   */
  private MetricQueryResults cachedMetricResults = null;

  /**
   * Constructor for the DataflowMetrics class.
   * @param dataflowPipelineJob is used to get Job state and Job ID information.
   * @param dataflowClient is used to query user metrics from the Dataflow service.
   */
  public DataflowMetrics(DataflowPipelineJob dataflowPipelineJob, DataflowClient dataflowClient) {
    this.dataflowClient = dataflowClient;
    this.dataflowPipelineJob = dataflowPipelineJob;
  }

  /**
   * Build an immutable map that serves as a hash key for a metric update.
   * @return a {@link MetricKey} that can be hashed and used to identify a metric.
   */
  private MetricKey metricHashKey(
      com.google.api.services.dataflow.model.MetricUpdate metricUpdate) {
    String fullStepName = metricUpdate.getName().getContext().get("step");
    fullStepName = (dataflowPipelineJob.aggregatorTransforms != null
        ? dataflowPipelineJob.aggregatorTransforms
            .getAppliedTransformForStepName(fullStepName).getFullName() : fullStepName);
    return MetricKey.create(
        fullStepName,
        MetricName.named(
            metricUpdate.getName().getContext().get("namespace"),
            metricUpdate.getName().getName()));
  }

  /**
   * Check whether a {@link com.google.api.services.dataflow.model.MetricUpdate} is a tentative
   * update or not.
   * @return true if update is tentative, false otherwise
   */
  private boolean isMetricTentative(
      com.google.api.services.dataflow.model.MetricUpdate metricUpdate) {
    return (metricUpdate.getName().getContext().containsKey("tentative")
        && Objects.equal(metricUpdate.getName().getContext().get("tentative"), "true"));
  }

  /**
   * Take a list of metric updates coming from the Dataflow service, and format it into a
   * Metrics API MetricQueryResults instance.
   * @param metricUpdates
   * @return a populated MetricQueryResults object.
   */
  private MetricQueryResults populateMetricQueryResults(
      List<com.google.api.services.dataflow.model.MetricUpdate> metricUpdates,
      MetricsFilter filter) {
    // Separate metric updates by name and by tentative/committed.
    HashMap<MetricKey, com.google.api.services.dataflow.model.MetricUpdate>
        tentativeByName = new HashMap<>();
    HashMap<MetricKey, com.google.api.services.dataflow.model.MetricUpdate>
        committedByName = new HashMap<>();
    HashSet<MetricKey> metricHashKeys = new HashSet<>();

    // If the Context of the metric update does not have a namespace, then these are not
    // actual metrics counters.
    for (com.google.api.services.dataflow.model.MetricUpdate update : metricUpdates) {
      if (Objects.equal(update.getName().getOrigin(), "user") && isMetricTentative(update)
          && update.getName().getContext().containsKey("namespace")) {
        tentativeByName.put(metricHashKey(update), update);
        metricHashKeys.add(metricHashKey(update));
      } else if (Objects.equal(update.getName().getOrigin(), "user")
          && update.getName().getContext().containsKey("namespace")
          && !isMetricTentative(update)) {
        committedByName.put(metricHashKey(update), update);
        metricHashKeys.add(metricHashKey(update));
      }
    }
    // Create the lists with the metric result information.
    ImmutableList.Builder<MetricResult<Long>> counterResults = ImmutableList.builder();
    ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults =
        ImmutableList.builder();
    ImmutableList.Builder<MetricResult<GaugeResult>> gaugeResults = ImmutableList.builder();
    for (MetricKey metricKey : metricHashKeys) {
      String metricName = metricKey.metricName().name();
      if (metricName.endsWith("[MIN]") || metricName.endsWith("[MAX]")
          || metricName.endsWith("[MEAN]") || metricName.endsWith("[COUNT]")) {
        // Skip distribution metrics, as these are not yet properly supported.
        LOG.warn("Distribution metrics are not yet supported. You can see them in the Dataflow"
            + "User Interface");
        continue;
      }
      String namespace = metricKey.metricName().namespace();
      String step = metricKey.stepName();
      Long committed = ((Number) committedByName.get(metricKey).getScalar()).longValue();
      Long attempted = ((Number) tentativeByName.get(metricKey).getScalar()).longValue();
      if (MetricFiltering.matches(filter, metricKey)) {
        counterResults.add(DataflowMetricResult.create(
            MetricName.named(namespace, metricName),
            step, committed, attempted));
      }
    }
    return DataflowMetricQueryResults.create(
        counterResults.build(),
        distributionResults.build(),
        gaugeResults.build());
  }

  private MetricQueryResults queryServiceForMetrics(MetricsFilter filter) {
    List<com.google.api.services.dataflow.model.MetricUpdate> metricUpdates;
    ImmutableList<MetricResult<Long>> counters = ImmutableList.of();
    ImmutableList<MetricResult<DistributionResult>> distributions = ImmutableList.of();
    ImmutableList<MetricResult<GaugeResult>> gauges = ImmutableList.of();
    JobMetrics jobMetrics;
    try {
      jobMetrics = dataflowClient.getJobMetrics(dataflowPipelineJob.jobId);
    } catch (IOException e) {
      LOG.warn("Unable to query job metrics.\n");
      return DataflowMetricQueryResults.create(counters, distributions, gauges);
    }
    metricUpdates = firstNonNull(
        jobMetrics.getMetrics(),
        Collections.<com.google.api.services.dataflow.model.MetricUpdate>emptyList());
    return populateMetricQueryResults(metricUpdates, filter);
  }

  public MetricQueryResults queryMetrics() {
    return queryMetrics(null);
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    if (cachedMetricResults != null) {
      // Metric results have been cached after the job ran.
      return cachedMetricResults;
    }
    MetricQueryResults result = queryServiceForMetrics(filter);
    if (dataflowPipelineJob.getState().isTerminal()) {
      // Add current query result to the cache.
      cachedMetricResults = result;
    }
    return result;
  }

  @AutoValue
  abstract static class DataflowMetricQueryResults implements MetricQueryResults {
    public static MetricQueryResults create(
        Iterable<MetricResult<Long>> counters,
        Iterable<MetricResult<DistributionResult>> distributions,
        Iterable<MetricResult<GaugeResult>> gauges) {
      return
          new AutoValue_DataflowMetrics_DataflowMetricQueryResults(counters, distributions, gauges);
    }
  }

  @AutoValue
  abstract static class DataflowMetricResult<T> implements MetricResult<T> {
    // need to define these here so they appear in the correct order
    // and the generated constructor is usable and consistent
    public abstract MetricName name();
    public abstract String step();
    public abstract T committed();
    public abstract T attempted();

    public static <T> MetricResult<T> create(MetricName name, String scope,
        T committed, T attempted) {
      return new AutoValue_DataflowMetrics_DataflowMetricResult<T>(
          name, scope, committed, attempted);
    }
  }
}
