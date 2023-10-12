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
package org.apache.beam.it.gcp;

import static org.apache.beam.it.common.logging.LogStrings.formatForLogging;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.RUNNER_V2;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.monitoring.MonitoringClient;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for performance tests. It provides helper methods for common operations. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public abstract class LoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LoadTestBase.class);
  // Dataflow resources cost factors (showing us-central-1 pricing).
  // See https://cloud.google.com/dataflow/pricing#pricing-details
  private static final double VCPU_PER_HR_BATCH = 0.056;
  private static final double VCPU_PER_HR_STREAMING = 0.069;
  private static final double MEM_PER_GB_HR_BATCH = 0.003557;
  private static final double MEM_PER_GB_HR_STREAMING = 0.0035557;
  private static final double PD_PER_GB_HR = 0.000054;
  private static final double PD_SSD_PER_GB_HR = 0.000298;
  private static final double SHUFFLE_PER_GB_BATCH = 0.011;
  private static final double SHUFFLE_PER_GB_STREAMING = 0.018;
  private static final Pattern WORKER_START_PATTERN =
      Pattern.compile(
          "^All workers have finished the startup processes and began to receive work requests.*$");
  private static final Pattern WORKER_STOP_PATTERN = Pattern.compile("^Stopping worker pool.*$");

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);

  @SuppressFBWarnings("MS_PKGPROTECT")
  protected static String project;

  @SuppressFBWarnings("MS_PKGPROTECT")
  protected static String region;

  protected MonitoringClient monitoringClient;
  protected PipelineLauncher pipelineLauncher;
  protected PipelineOperator pipelineOperator;

  protected String testName;

  @Rule
  public TestRule watcher =
      new TestWatcher() {
        @Override
        protected void starting(Description description) {
          LOG.info(
              "Starting load test {}.{}", description.getClassName(), description.getMethodName());
          testName = description.getMethodName();
        }
      };

  @BeforeClass
  public static void setUpClass() {
    project = TestProperties.project();
    region = TestProperties.region();
  }

  @Before
  public void setUp() throws IOException {
    monitoringClient = MonitoringClient.builder(CREDENTIALS_PROVIDER).build();
    pipelineLauncher = launcher();
    pipelineOperator = new PipelineOperator(pipelineLauncher);
  }

  @After
  public void tearDownLoadTestBase() throws IOException {
    pipelineLauncher.cleanupAll();
    monitoringClient.cleanupAll();
  }

  public abstract PipelineLauncher launcher();

  /**
   * Exports the metrics of given dataflow job to BigQuery.
   *
   * @param launchInfo Job info of the job
   * @param metrics metrics to export
   */
  protected void exportMetricsToBigQuery(LaunchInfo launchInfo, Map<String, Double> metrics) {
    LOG.info("Exporting metrics:\n{}", formatForLogging(metrics));
    try {
      // either use the user specified project for exporting, or the same project
      String exportProject = MoreObjects.firstNonNull(TestProperties.exportProject(), project);
      BigQueryResourceManager bigQueryResourceManager =
          BigQueryResourceManager.builder(testName, exportProject, CREDENTIALS)
              .setDatasetId(TestProperties.exportDataset())
              .build();
      // exporting metrics to bigQuery table
      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("timestamp", launchInfo.createTime());
      rowContent.put("sdk", launchInfo.sdk());
      rowContent.put("version", launchInfo.version());
      rowContent.put("job_type", launchInfo.jobType());
      putOptional(rowContent, "template_name", launchInfo.templateName());
      putOptional(rowContent, "template_version", launchInfo.templateVersion());
      putOptional(rowContent, "template_type", launchInfo.templateType());
      putOptional(rowContent, "pipeline_name", launchInfo.pipelineName());
      rowContent.put("test_name", testName);
      // Convert parameters map to list of table row since it's a repeated record
      List<TableRow> parameterRows = new ArrayList<>();
      for (Entry<String, String> entry : launchInfo.parameters().entrySet()) {
        TableRow row = new TableRow().set("name", entry.getKey()).set("value", entry.getValue());
        parameterRows.add(row);
      }
      rowContent.put("parameters", parameterRows);
      // Convert metrics map to list of table row since it's a repeated record
      List<TableRow> metricRows = new ArrayList<>();
      for (Entry<String, Double> entry : metrics.entrySet()) {
        TableRow row = new TableRow().set("name", entry.getKey()).set("value", entry.getValue());
        metricRows.add(row);
      }
      rowContent.put("metrics", metricRows);
      bigQueryResourceManager.write(
          TestProperties.exportTable(), RowToInsert.of("rowId", rowContent));
    } catch (IllegalStateException e) {
      LOG.error("Unable to export results to datastore. ", e);
    }
  }

  /**
   * Checks if the input PCollection has the expected number of messages.
   *
   * @param jobId JobId of the job
   * @param pcollection the input pcollection name
   * @param expectedElements expected number of messages
   * @return whether the input pcollection has the expected number of messages.
   */
  protected boolean waitForNumMessages(String jobId, String pcollection, Long expectedElements) {
    try {
      // the element count metric always follows the pattern <pcollection name>-ElementCount
      String metricName = pcollection.replace(".", "-") + "-ElementCount";
      Double metric = pipelineLauncher.getMetric(project, region, jobId, metricName);
      if ((metric != null) && (metric >= (double) expectedElements)) {
        return true;
      }
      LOG.info(
          "Expected {} messages in input PCollection, but found {}.", expectedElements, metric);
      return false;
    } catch (Exception e) {
      LOG.warn("Encountered error when trying to measure input elements. ", e);
      return false;
    }
  }

  /**
   * Compute metrics of a Dataflow runner job.
   *
   * @param metrics a map of raw metrics. The results are also appened in the map.
   * @param launchInfo Job info of the job
   * @param config a {@class MetricsConfiguration}
   */
  private void computeDataflowMetrics(
      Map<String, Double> metrics, LaunchInfo launchInfo, MetricsConfiguration config)
      throws ParseException {
    // cost info
    LOG.info("Calculating approximate cost for {} under {}", launchInfo.jobId(), project);
    TimeInterval workerTimeInterval = getWorkerTimeInterval(launchInfo);
    metrics.put(
        "RunTime",
        (double)
            Timestamps.between(workerTimeInterval.getStartTime(), workerTimeInterval.getEndTime())
                .getSeconds());
    double cost = 0;
    if (launchInfo.jobType().equals("JOB_TYPE_STREAMING")) {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_STREAMING;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_STREAMING;
      cost += metrics.get("TotalStreamingDataProcessed") * SHUFFLE_PER_GB_STREAMING;
      // Also, add other streaming metrics
      metrics.putAll(getDataFreshnessMetrics(launchInfo.jobId(), workerTimeInterval));
      metrics.putAll(getSystemLatencyMetrics(launchInfo.jobId(), workerTimeInterval));
    } else {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_BATCH;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_BATCH;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_BATCH;
    }
    cost += metrics.get("TotalPdUsage") / 3600 * PD_PER_GB_HR;
    cost += metrics.get("TotalSsdUsage") / 3600 * PD_SSD_PER_GB_HR;
    metrics.put("EstimatedCost", cost);
    metrics.put("ElapsedTime", monitoringClient.getElapsedTime(project, launchInfo));

    Double dataProcessed =
        monitoringClient.getDataProcessed(project, launchInfo, config.inputPCollection());
    if (dataProcessed != null) {
      metrics.put("EstimatedDataProcessedGB", dataProcessed / 1e9d);
    }
    metrics.putAll(getCpuUtilizationMetrics(launchInfo.jobId(), workerTimeInterval));
    metrics.putAll(getThroughputMetrics(launchInfo, config, workerTimeInterval));
  }

  /**
   * Compute metrics of a Direct runner job.
   *
   * @param metrics a map of raw metrics. The results are also appened in the map.
   * @param launchInfo Job info of the job
   */
  private void computeDirectMetrics(Map<String, Double> metrics, LaunchInfo launchInfo)
      throws ParseException {
    // TODO: determine elapsed time more accurately if Direct runner supports do so.
    metrics.put(
        "ElapsedTime",
        0.001
            * (System.currentTimeMillis()
                - Timestamps.toMillis(Timestamps.parse(launchInfo.createTime()))));
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @param outputPcollection output pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(
      LaunchInfo launchInfo, @Nullable String inputPcollection, @Nullable String outputPcollection)
      throws InterruptedException, IOException, ParseException {
    return getMetrics(
        launchInfo,
        MetricsConfiguration.builder()
            .setInputPCollection(inputPcollection)
            .setOutputPCollection(outputPcollection)
            .build());
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(LaunchInfo launchInfo, String inputPcollection)
      throws ParseException, InterruptedException, IOException {
    return getMetrics(launchInfo, inputPcollection, null);
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(LaunchInfo launchInfo)
      throws ParseException, InterruptedException, IOException {
    return getMetrics(launchInfo, null, null);
  }

  protected Map<String, Double> getMetrics(LaunchInfo launchInfo, MetricsConfiguration config)
      throws IOException, InterruptedException, ParseException {
    Map<String, Double> metrics = pipelineLauncher.getMetrics(project, region, launchInfo.jobId());
    if (launchInfo.runner().contains("Dataflow")) {
      // monitoring metrics take up to 3 minutes to show up
      // TODO(pranavbhandari): We should use a library like http://awaitility.org/ to poll for
      // metrics instead of hard coding X minutes.
      LOG.info("Sleeping for 4 minutes to query Dataflow runner metrics.");
      Thread.sleep(Duration.ofMinutes(4).toMillis());
      computeDataflowMetrics(metrics, launchInfo, config);
    } else if ("DirectRunner".equalsIgnoreCase(launchInfo.runner())) {
      computeDirectMetrics(metrics, launchInfo);
    }
    return metrics;
  }

  /**
   * Computes CPU Utilization metrics of the given job.
   *
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return CPU Utilization metrics of the job
   */
  protected Map<String, Double> getCpuUtilizationMetrics(String jobId, TimeInterval timeInterval) {
    List<Double> cpuUtilization = monitoringClient.getCpuUtilization(project, jobId, timeInterval);
    Map<String, Double> cpuUtilizationMetrics = new HashMap<>();
    if (cpuUtilization == null) {
      return cpuUtilizationMetrics;
    }
    cpuUtilizationMetrics.put("AvgCpuUtilization", calculateAverage(cpuUtilization));
    cpuUtilizationMetrics.put("MaxCpuUtilization", Collections.max(cpuUtilization));
    return cpuUtilizationMetrics;
  }

  /**
   * Computes throughput metrics of the given pcollection in dataflow job.
   *
   * @param jobInfo dataflow job LaunchInfo
   * @param config the {@class MetricsConfiguration}
   * @param timeInterval interval for the monitoring query
   * @return throughput metrics of the pcollection
   */
  protected Map<String, Double> getThroughputMetrics(
      LaunchInfo jobInfo, MetricsConfiguration config, TimeInterval timeInterval) {
    String jobId = jobInfo.jobId();
    String iColl =
        RUNNER_V2.equals(jobInfo.runner())
            ? config.inputPCollectionV2()
            : config.inputPCollection();
    String oColl =
        RUNNER_V2.equals(jobInfo.runner())
            ? config.outputPCollectionV2()
            : config.outputPCollection();
    List<Double> inputThroughputBytesPerSec =
        monitoringClient.getThroughputBytesPerSecond(project, jobId, iColl, timeInterval);
    List<Double> inputThroughputElementsPerSec =
        monitoringClient.getThroughputElementsPerSecond(project, jobId, iColl, timeInterval);
    List<Double> outputThroughputBytesPerSec =
        monitoringClient.getThroughputBytesPerSecond(project, jobId, oColl, timeInterval);
    List<Double> outputThroughputElementsPerSec =
        monitoringClient.getThroughputElementsPerSecond(project, jobId, oColl, timeInterval);
    return getThroughputMetrics(
        inputThroughputBytesPerSec,
        inputThroughputElementsPerSec,
        outputThroughputBytesPerSec,
        outputThroughputElementsPerSec);
  }

  /**
   * Computes Data freshness metrics of the given job.
   *
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return Data freshness metrics of the job
   */
  protected Map<String, Double> getDataFreshnessMetrics(String jobId, TimeInterval timeInterval) {
    List<Double> dataFreshness = monitoringClient.getDataFreshness(project, jobId, timeInterval);
    Map<String, Double> dataFreshnessMetrics = new HashMap<>();
    if (dataFreshness == null) {
      return dataFreshnessMetrics;
    }
    dataFreshnessMetrics.put("AvgDataFreshness", calculateAverage(dataFreshness));
    dataFreshnessMetrics.put("MaxDataFreshness", Collections.max(dataFreshness));
    return dataFreshnessMetrics;
  }

  /**
   * Computes System latency metrics of the given job.
   *
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return System latency metrics of the job
   */
  protected Map<String, Double> getSystemLatencyMetrics(String jobId, TimeInterval timeInterval) {
    List<Double> systemLatency = monitoringClient.getSystemLatency(project, jobId, timeInterval);
    Map<String, Double> systemLatencyMetrics = new HashMap<>();
    if (systemLatency == null) {
      return systemLatencyMetrics;
    }
    systemLatencyMetrics.put("AvgSystemLatency", calculateAverage(systemLatency));
    systemLatencyMetrics.put("MaxSystemLatency", Collections.max(systemLatency));
    return systemLatencyMetrics;
  }

  /** Gets the time interval when workers were active to be used by monitoring client. */
  protected TimeInterval getWorkerTimeInterval(LaunchInfo info) throws ParseException {
    TimeInterval.Builder builder = TimeInterval.newBuilder();
    List<JobMessage> messages =
        pipelineLauncher.listMessages(project, region, info.jobId(), "JOB_MESSAGE_DETAILED");
    for (JobMessage jobMessage : messages) {
      if (jobMessage.getMessageText() != null && !jobMessage.getMessageText().isEmpty()) {
        if (WORKER_START_PATTERN.matcher(jobMessage.getMessageText()).find()) {
          LOG.info("Found worker start message in job messages.");
          builder.setStartTime(Timestamps.parse(jobMessage.getTime()));
        }
        if (WORKER_STOP_PATTERN.matcher(jobMessage.getMessageText()).find()) {
          LOG.info("Found worker stop message in job messages.");
          builder.setEndTime(Timestamps.parse(jobMessage.getTime()));
        }
      }
    }
    return builder.build();
  }

  private Map<String, Double> getThroughputMetrics(
      List<Double> inputThroughput,
      List<Double> inputThroughputElementsPerSec,
      List<Double> outputThroughput,
      List<Double> outputThroughputElementsPerSec) {
    Map<String, Double> throughputMetrics = new HashMap<>();
    if (inputThroughput != null) {
      throughputMetrics.put("AvgInputThroughputBytesPerSec", calculateAverage(inputThroughput));
      throughputMetrics.put("MaxInputThroughputBytesPerSec", Collections.max(inputThroughput));
    }
    if (inputThroughputElementsPerSec != null) {
      throughputMetrics.put(
          "AvgInputThroughputElementsPerSec", calculateAverage(inputThroughputElementsPerSec));
      throughputMetrics.put(
          "MaxInputThroughputElementsPerSec", Collections.max(inputThroughputElementsPerSec));
    }
    if (outputThroughput != null) {
      throughputMetrics.put("AvgOutputThroughputBytesPerSec", calculateAverage(outputThroughput));
      throughputMetrics.put("MaxOutputThroughputBytesPerSec", Collections.max(outputThroughput));
    }
    if (outputThroughputElementsPerSec != null) {
      throughputMetrics.put(
          "AvgOutputThroughputElementsPerSec", calculateAverage(outputThroughputElementsPerSec));
      throughputMetrics.put(
          "MaxOutputThroughputElementsPerSec", Collections.max(outputThroughputElementsPerSec));
    }
    return throughputMetrics;
  }

  /**
   * Calculate the average from a series.
   *
   * @param values the input series.
   * @return the averaged result.
   */
  public static Double calculateAverage(List<Double> values) {
    return values.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  private static void putOptional(Map<String, Object> map, String key, @Nullable Object value) {
    if (value != null) {
      map.put(key, value);
    }
  }

  public static PipelineOperator.Config createConfig(LaunchInfo info, Duration timeout) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(project)
        .setRegion(region)
        .setTimeoutAfter(timeout)
        .build();
  }

  /** Utils for the metrics. */
  @AutoValue
  public abstract static class MetricsConfiguration {

    /**
     * Input PCollection of the Dataflow job to query additional metrics. If not provided, the
     * metrics for inputPCollection will not be calculated.
     */
    public abstract @Nullable String inputPCollection();

    /** Input PCollection name under Dataflow runner v2. */
    public abstract @Nullable String inputPCollectionV2();

    /**
     * Input PCollection of the Dataflow job to query additional metrics. If not provided, the
     * metrics for inputPCollection will not be calculated.
     */
    public abstract @Nullable String outputPCollection();

    public abstract @Nullable String outputPCollectionV2();

    public static MetricsConfiguration.Builder builder() {
      return new AutoValue_LoadTestBase_MetricsConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract MetricsConfiguration.Builder setInputPCollection(@Nullable String value);

      public abstract MetricsConfiguration.Builder setInputPCollectionV2(@Nullable String value);

      public abstract MetricsConfiguration.Builder setOutputPCollection(@Nullable String value);

      public abstract MetricsConfiguration.Builder setOutputPCollectionV2(@Nullable String value);

      public abstract MetricsConfiguration build();
    }
  }
}
