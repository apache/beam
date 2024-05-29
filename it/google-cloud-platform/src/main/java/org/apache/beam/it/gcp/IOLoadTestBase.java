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

import com.google.cloud.Timestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.dataflow.DefaultPipelineLauncher;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for IO Load tests. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public class IOLoadTestBase extends LoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IOLoadTestBase.class);

  protected String tempBucketName;

  @Before
  public void setUpBase() {
    // Prefer artifactBucket, but use the staging one if none given
    if (TestProperties.hasArtifactBucket()) {
      tempBucketName = TestProperties.artifactBucket();
    } else if (TestProperties.hasStageBucket()) {
      tempBucketName = TestProperties.stageBucket();
    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. Pipeline may fail if a temp gcs"
              + " location is needed");
    }
  }

  @After
  public void tearDownBase() throws IOException {
    pipelineLauncher.cleanupAll();
  }

  @Override
  public PipelineLauncher launcher() {
    return DefaultPipelineLauncher.builder(CREDENTIALS).build();
  }

  /** A utility DoFn that counts elements passed through. */
  public static final class CountingFn<T> extends DoFn<T, T> {

    private final Counter elementCounter;

    public CountingFn(String name) {
      elementCounter = Metrics.counter(BEAM_METRICS_NAMESPACE, name);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      elementCounter.inc(1L);
      ctx.output(ctx.element());
    }
  }

  // To make PipelineLauncher.getMetric work in a unified way for both runner provided metrics and
  // pipeline defined
  // metrics, here we wrap Beam provided metrics as a pre-defined metrics name
  // [name_space:metric_type:metric_name
  // which will be recognized by getMetric method
  public enum PipelineMetricsType {
    COUNTER,
    STARTTIME,
    ENDTIME,
    RUNTIME,
  }

  /** Namespace for Beam provided pipeline metrics (set up by Metrics transform). */
  public static final String BEAM_METRICS_NAMESPACE = "BEAM_METRICS";

  /** Given a metrics name, return Beam metrics name. */
  public static String getBeamMetricsName(PipelineMetricsType metricstype, String metricsName) {
    return BEAM_METRICS_NAMESPACE + ":" + metricstype + ":" + metricsName;
  }

  /** Exports test metrics to InfluxDB or BigQuery depending on the configuration. */
  protected void exportMetrics(
      PipelineLauncher.LaunchInfo launchInfo,
      MetricsConfiguration metricsConfig,
      boolean exportToInfluxDB,
      InfluxDBSettings influxDBSettings) {

    Map<String, Double> metrics;
    try {
      metrics = getMetrics(launchInfo, metricsConfig);
    } catch (Exception e) {
      LOG.warn("Unable to get metrics due to error: {}", e.getMessage());
      return;
    }
    String testId = UUID.randomUUID().toString();
    String testTimestamp = Timestamp.now().toString();

    if (exportToInfluxDB) {
      Collection<NamedTestResult> namedTestResults = new ArrayList<>();
      for (Map.Entry<String, Double> entry : metrics.entrySet()) {
        NamedTestResult metricResult =
            NamedTestResult.create(testId, testTimestamp, entry.getKey(), entry.getValue());
        namedTestResults.add(metricResult);
      }
      IOITMetrics.publishToInflux(testId, testTimestamp, namedTestResults, influxDBSettings);
    } else {
      exportMetricsToBigQuery(launchInfo, metrics);
    }
  }
}
