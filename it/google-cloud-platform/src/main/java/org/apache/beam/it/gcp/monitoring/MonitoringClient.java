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
package org.apache.beam.it.gcp.monitoring;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.Aggregation.Aligner;
import com.google.monitoring.v3.Aggregation.Reducer;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for working with Google Cloud Monitoring. */
public final class MonitoringClient {
  private static final Logger LOG = LoggerFactory.getLogger(MonitoringClient.class);
  private final MetricServiceClient metricServiceClient;

  private MonitoringClient(Builder builder) throws IOException {
    MetricServiceSettings metricServiceSettings =
        MetricServiceSettings.newBuilder()
            .setCredentialsProvider(builder.getCredentialsProvider())
            .build();
    this.metricServiceClient = MetricServiceClient.create(metricServiceSettings);
  }

  private MonitoringClient(MetricServiceClient metricServiceClient) {
    this.metricServiceClient = metricServiceClient;
  }

  public static MonitoringClient withMonitoringClient(MetricServiceClient metricServiceClient) {
    return new MonitoringClient(metricServiceClient);
  }

  public static Builder builder(CredentialsProvider credentialsProvider) {
    return new Builder(credentialsProvider);
  }

  /**
   * Lists time series that match a filter.
   *
   * @param request time series request to execute
   * @return list of Double values of time series
   */
  public List<Double> listTimeSeriesAsDouble(ListTimeSeriesRequest request) {
    return extractValuesFromTimeSeriesAsDouble(metricServiceClient.listTimeSeries(request));
  }

  /**
   * Lists time series that match a filter.
   *
   * @param request time series request to execute
   * @return list of Long values of time series
   */
  public List<Long> listTimeSeriesAsLong(ListTimeSeriesRequest request) {
    return extractValuesFromTimeSeriesAsLong(metricServiceClient.listTimeSeries(request));
  }

  /**
   * Gets the number of undelivered messages in a given Pub/Sub subscription.
   *
   * @param project the project that the job is running under
   * @param subscriptionName name of the Pub/Sub subscription
   * @return number of undelivered messages in the subscription
   */
  public @Nullable Long getNumMessagesInSubscription(String project, String subscriptionName) {
    LOG.info(
        "Getting number of messages in subscription for {} under {}", subscriptionName, project);
    String filter =
        String.format(
            "metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" "
                + "AND resource.labels.subscription_id = \"%s\"",
            subscriptionName);
    // only consider the time interval of last 60 seconds to get the most recent value
    TimeInterval timeInterval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.fromMillis(System.currentTimeMillis() - 60000))
            .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MAX)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Long> timeSeries = listTimeSeriesAsLong(request);
    if (timeSeries.isEmpty()) {
      LOG.warn(
          "No monitoring data found. Unable to get number of messages in {}.", subscriptionName);
      return null;
    }
    // get the last recorded value of number of messages in the subscription (there should be only 1
    // value)
    return timeSeries.get(0);
  }

  /**
   * Gets the CPU Utilization time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return CPU Utilization time series data for the given job.
   */
  public @Nullable List<Double> getCpuUtilization(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting CPU utilization for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"compute.googleapis.com/instance/cpu/utilization\" "
                + "AND resource.labels.project_id = \"%s\" "
                + "AND metadata.user_labels.dataflow_job_id = \"%s\"",
            project, jobId);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Reducer.REDUCE_MEAN)
            .addGroupByFields("resource.instance_id")
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get CPU utilization information.");
      return null;
    }
    return timeSeries;
  }

  /**
   * Gets the System Latency time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return System Latency time series data for the given job.
   */
  public @Nullable List<Double> getSystemLatency(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting system latency for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/per_stage_system_lag\" "
                + "AND metric.labels.job_id = \"%s\"",
            jobId);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Reducer.REDUCE_MAX)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get System Latency information.");
      return null;
    }
    return timeSeries;
  }

  /**
   * Gets the Data freshness time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return Data freshness time series data for the given job.
   */
  public @Nullable List<Double> getDataFreshness(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting data freshness for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/per_stage_data_watermark_age\" "
                + "AND metric.labels.job_id = \"%s\"",
            jobId);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Reducer.REDUCE_MAX)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get Data freshness information.");
      return null;
    }
    return timeSeries;
  }

  /**
   * Gets the output throughput in bytes per second from a particular PCollection during job run
   * interval.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param pcollection name of the pcollection
   * @param timeInterval interval for the monitoring query
   * @return output throughput from a particular PCollection during job run interval
   */
  public @Nullable List<Double> getThroughputBytesPerSecond(
      String project, String jobId, String pcollection, TimeInterval timeInterval) {
    if (pcollection == null) {
      LOG.warn("Output PCollection name not provided. Unable to calculate throughput.");
      return null;
    }
    LOG.info("Getting throughput (bytes/sec) for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/estimated_bytes_produced_count\" "
                + "AND metric.labels.job_id=\"%s\" "
                + "AND metric.labels.pcollection=\"%s\" ",
            jobId, pcollection);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_RATE)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get throughput information.");
      return null;
    }
    return timeSeries;
  }

  /**
   * Gets the output throughput in elements per second from a particular PCollection during job run
   * interval.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param pcollection name of the pcollection
   * @param timeInterval interval for the monitoring query
   * @return output throughput from a particular PCollection during job run interval
   */
  public @Nullable List<Double> getThroughputElementsPerSecond(
      String project, String jobId, String pcollection, TimeInterval timeInterval) {
    if (pcollection == null) {
      LOG.warn("Output PCollection name not provided. Unable to calculate throughput.");
      return null;
    }
    LOG.info("Getting throughput (elements/sec) for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/elements_produced_count\" "
                + "AND metric.labels.job_id=\"%s\" "
                + "AND metric.labels.pcollection=\"%s\" ",
            jobId, pcollection);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_RATE)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get throughput information.");
      return null;
    }
    return timeSeries;
  }

  /**
   * Get elapsed time for a job.
   *
   * @param project the project that the job is running under
   * @param launchInfo information about the job
   * @return elapsed time
   * @throws ParseException if timestamp is inaccurate
   */
  public @Nullable Double getElapsedTime(String project, LaunchInfo launchInfo)
      throws ParseException {
    LOG.info("Getting elapsed time for {} under {}", launchInfo.jobId(), project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/elapsed_time\" "
                + "AND metric.labels.job_id=\"%s\" ",
            launchInfo.jobId());
    TimeInterval timeInterval = getTimeInterval(launchInfo.createTime());
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MEAN)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get elapsed time information.");
      return null;
    }
    // getting max since this is a gauge metric
    return Collections.max(timeSeries);
  }

  /**
   * Get data processed for a job.
   *
   * @param project the project that the job is running under
   * @param launchInfo information about the job
   * @return data processed
   * @throws ParseException if timestamp is inaccurate
   */
  public @Nullable Double getDataProcessed(
      String project, LaunchInfo launchInfo, String pCollection) throws ParseException {
    if (pCollection == null) {
      LOG.warn("PCollection name not provided. Unable to calculate data processed.");
      return null;
    }
    LOG.info("Getting data processed for {} under {}", launchInfo.jobId(), project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/estimated_byte_count\" "
                + "AND metric.labels.job_id=\"%s\" "
                + "AND metric.labels.pcollection=\"%s\" ",
            launchInfo.jobId(), pCollection);
    TimeInterval timeInterval = getTimeInterval(launchInfo.createTime());
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aligner.ALIGN_MEAN)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get data processed information.");
      return null;
    }
    // getting max since this is a gauge metric
    return Collections.max(timeSeries);
  }

  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup monitoring client.");
    metricServiceClient.close();
    LOG.info("Monitoring client successfully cleaned up.");
  }

  private TimeInterval getTimeInterval(String startTime) throws ParseException {
    return TimeInterval.newBuilder()
        .setStartTime(Timestamps.parse(startTime))
        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }

  private List<Double> extractValuesFromTimeSeriesAsDouble(ListTimeSeriesPagedResponse response) {
    List<Double> values = new ArrayList<>();
    for (TimeSeries ts : response.iterateAll()) {
      for (Point point : ts.getPointsList()) {
        values.add(point.getValue().getDoubleValue());
      }
    }
    return values;
  }

  private List<Long> extractValuesFromTimeSeriesAsLong(ListTimeSeriesPagedResponse response) {
    List<Long> values = new ArrayList<>();
    for (TimeSeries ts : response.iterateAll()) {
      for (Point point : ts.getPointsList()) {
        values.add(point.getValue().getInt64Value());
      }
    }
    return values;
  }

  /** Builder for {@link MonitoringClient}. */
  public static final class Builder {
    private CredentialsProvider credentialsProvider;

    private Builder(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
    }

    public CredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    public Builder setCredentialsProvider(CredentialsProvider value) {
      credentialsProvider = value;
      return this;
    }

    public MonitoringClient build() throws IOException {
      return new MonitoringClient(this);
    }
  }
}
