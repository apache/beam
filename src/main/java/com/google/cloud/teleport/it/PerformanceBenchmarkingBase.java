/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it;

import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DefaultDataflowClient;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.monitoring.MonitoringClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for testing performance of dataflow templates. */
@RunWith(JUnit4.class)
public class PerformanceBenchmarkingBase {
  private static final Logger LOG = LoggerFactory.getLogger(PerformanceBenchmarkingBase.class);
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
  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();
  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);
  protected static MonitoringClient monitoringClient;
  protected static DataflowClient dataflowClient;
  protected static DataflowOperator dataflowOperator;

  @Rule public final TestName testName = new TestName();

  @Before
  public void setUpPerformanceBenchmarkingBase() throws IOException {
    monitoringClient =
        MonitoringClient.builder().setCredentialsProvider(CREDENTIALS_PROVIDER).build();
    dataflowClient = getDataflowClient();
    dataflowOperator = new DataflowOperator(dataflowClient);
  }

  @After
  public void tearDownPerformanceBenchmarkingBase() {
    monitoringClient.cleanupAll();
  }

  protected DataflowClient getDataflowClient() {
    // If there is a TemplateIntegrationTest annotation, return appropriate dataflow template client
    // Else, return default dataflow client.
    TemplateIntegrationTest annotation = getClass().getAnnotation(TemplateIntegrationTest.class);
    if (annotation == null) {
      LOG.warn(
          "{} did not specify which template is tested using @TemplateIntegrationTest, using DefaultDataflowClient.",
          getClass());
      return DefaultDataflowClient.builder().setCredentials(CREDENTIALS).build();
    }

    Class<?> templateClass = annotation.value();
    Template[] templateAnnotations = templateClass.getAnnotationsByType(Template.class);
    if (templateAnnotations.length == 0) {
      LOG.warn(
          " \"Template mentioned in @TemplateIntegrationTest for {} does not contain a @Template annotation, using DefaultDataflowClient.",
          getClass());
      return DefaultDataflowClient.builder().setCredentials(CREDENTIALS).build();
    } else if (templateAnnotations[0].flexContainerName() != null
        && !templateAnnotations[0].flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(CREDENTIALS).build();
    }
  }

  /**
   * Exports the metrics of given dataflow job to BigQuery.
   *
   * @param jobInfo Job info of the job
   * @param metrics metrics to export
   */
  public void exportMetricsToBigQuery(JobInfo jobInfo, Map<String, Double> metrics) {
    LOG.info("Exporting metrics:\n{}", formatForLogging(metrics));
    try {
      // either use the user specified project for exporting, or the same project
      String project = MoreObjects.firstNonNull(TestProperties.exportProject(), PROJECT);
      BigQueryResourceManager bigQueryResourceManager =
          DefaultBigQueryResourceManager.builder(testName.getMethodName(), project)
              .setDatasetId(TestProperties.exportDataset())
              .setCredentials(CREDENTIALS)
              .build();
      // exporting metrics to bigQuery table
      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("timestamp", jobInfo.createTime());
      rowContent.put("sdk", jobInfo.sdk());
      rowContent.put("version", jobInfo.version());
      rowContent.put("job_type", jobInfo.jobType());
      rowContent.put("template_name", jobInfo.templateName());
      rowContent.put("template_version", jobInfo.templateVersion());
      rowContent.put("template_type", jobInfo.templateType());
      rowContent.put("test_name", testName.getMethodName());
      // Convert parameters map to list of table row since it's a repeated record
      List<TableRow> parameterRows = new ArrayList<>();
      for (String parameter : jobInfo.parameters().keySet()) {
        TableRow row =
            new TableRow().set("name", parameter).set("value", jobInfo.parameters().get(parameter));
        parameterRows.add(row);
      }
      rowContent.put("parameters", parameterRows);
      // Convert metrics map to list of table row since it's a repeated record
      List<TableRow> metricRows = new ArrayList<>();
      for (String metric : metrics.keySet()) {
        TableRow row = new TableRow().set("name", metric).set("value", metrics.get(metric));
        metricRows.add(row);
      }
      rowContent.put("metrics", metricRows);
      bigQueryResourceManager.write(
          TestProperties.exportTable(), RowToInsert.of("rowId", rowContent));
    } catch (IllegalStateException e) {
      LOG.warn("Unable to export results to datastore. ", e);
    }
  }

  /**
   * Checks if the input PCollection has the expected number of messages.
   *
   * @param jobId JobId of the job
   * @param pcollectionName the input pcollection name
   * @param expectedElements expected number of messages
   * @return whether the input pcollection has the expected number of messages.
   */
  public boolean waitForNumMessages(String jobId, String pcollectionName, Long expectedElements) {
    try {
      // the element count metric always follows the pattern <pcollection name>-ElementCount
      String metricName = pcollectionName.replace(".", "-") + "-ElementCount";
      Double metric = dataflowClient.getMetric(PROJECT, REGION, jobId, metricName);
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
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param jobInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(JobInfo jobInfo, String inputPcollection)
      throws ParseException, InterruptedException, IOException {
    // Metrics take up to 3 minutes to show up
    Thread.sleep(Duration.ofMinutes(4).toMillis());
    Map<String, Double> metrics = dataflowClient.getMetrics(PROJECT, REGION, jobInfo.jobId());
    LOG.info("Calculating approximate cost for {} under {}", jobInfo.jobId(), PROJECT);
    double cost = 0;
    if (jobInfo.jobType().equals("JOB_TYPE_STREAMING")) {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_STREAMING;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_STREAMING;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_STREAMING;
    } else {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_BATCH;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_BATCH;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_BATCH;
    }
    cost += metrics.get("TotalPdUsage") / 3600 * PD_PER_GB_HR;
    cost += metrics.get("TotalSsdUsage") / 3600 * PD_SSD_PER_GB_HR;
    metrics.put("EstimatedCost", cost);
    metrics.put("ElapsedTime", monitoringClient.getElapsedTime(PROJECT, jobInfo));
    Double dataProcessed = monitoringClient.getDataProcessed(PROJECT, jobInfo, inputPcollection);
    if (dataProcessed != null) {
      metrics.put("EstimatedDataProcessedGB", dataProcessed / 1e9d);
      metrics.put("EstimatedCostPerGBProcessed", metrics.get("EstimatedCost") / dataProcessed);
    }
    return metrics;
  }

  /**
   * Computes CPU Utilization metrics of the given job.
   *
   * @param jobInfo Job info of the job
   * @return CPU Utilization metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getCpuUtilizationMetrics(JobInfo jobInfo) throws ParseException {
    List<Double> cpuUtilization = monitoringClient.getCpuUtilization(PROJECT, jobInfo);
    Map<String, Double> cpuUtilizationMetrics = new HashMap<>();
    if (cpuUtilization == null) {
      return cpuUtilizationMetrics;
    }
    cpuUtilizationMetrics.put("AvgCpuUtilization", calculateAverage(cpuUtilization));
    cpuUtilizationMetrics.put("MaxCpuUtilization", Collections.max(cpuUtilization));
    return cpuUtilizationMetrics;
  }

  /**
   * Computes throughput metrics of the given ptransform in dataflow job.
   *
   * @param jobInfo Job info of the job
   * @param ptransform ptransform of the dataflow job to query additional metrics
   * @return throughput metrics of the ptransform
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getThroughputMetricsOfPtransform(JobInfo jobInfo, String ptransform)
      throws ParseException {
    List<Double> throughput =
        monitoringClient.getThroughputOfPtransform(PROJECT, jobInfo, ptransform);
    return getThroughputMetrics(throughput);
  }

  /**
   * Computes throughput metrics of the given pcollection in dataflow job.
   *
   * @param jobInfo Job info of the job
   * @param pcollection pcollection of the dataflow job to query additional metrics
   * @return throughput metrics of the pcollection
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getThroughputMetricsOfPcollection(
      JobInfo jobInfo, String pcollection) throws ParseException {
    List<Double> throughput =
        monitoringClient.getThroughputOfPcollection(PROJECT, jobInfo, pcollection);
    return getThroughputMetrics(throughput);
  }

  /**
   * Computes Data freshness metrics of the given job.
   *
   * @param jobInfo Job info of the job
   * @return Data freshness metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getDataFreshnessMetrics(JobInfo jobInfo) throws ParseException {
    List<Double> dataFreshness = monitoringClient.getDataFreshness(PROJECT, jobInfo);
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
   * @param jobInfo Job info of the job
   * @return System latency metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getSystemLatencyMetrics(JobInfo jobInfo) throws ParseException {
    List<Double> systemLatency = monitoringClient.getSystemLatency(PROJECT, jobInfo);
    Map<String, Double> systemLatencyMetrics = new HashMap<>();
    if (systemLatency == null) {
      return systemLatencyMetrics;
    }
    systemLatencyMetrics.put("AvgSystemLatency", calculateAverage(systemLatency));
    systemLatencyMetrics.put("MaxSystemLatency", Collections.max(systemLatency));
    return systemLatencyMetrics;
  }

  private Map<String, Double> getThroughputMetrics(List<Double> throughput) {
    Map<String, Double> throughputMetrics = new HashMap<>();
    if (throughput == null) {
      return throughputMetrics;
    }
    throughputMetrics.put("AvgThroughput", calculateAverage(throughput));
    throughputMetrics.put("MaxThroughput", Collections.max(throughput));
    return throughputMetrics;
  }

  private Double calculateAverage(List<Double> values) {
    return values.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  public static DataflowOperator.Config createConfig(JobInfo info, Duration timeout) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .setTimeoutAfter(timeout)
        .build();
  }
}
