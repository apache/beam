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
package org.apache.beam.runners.jstorm;

import static com.google.common.base.Preconditions.checkNotNull;

import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.AsmMetricRegistry;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test JStorm runner.
 */
public class TestJStormRunner extends PipelineRunner<JStormRunnerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TestJStormRunner.class);

  public static TestJStormRunner fromOptions(PipelineOptions options) {
    return new TestJStormRunner(options.as(JStormPipelineOptions.class));
  }

  // waiting time when job with assertion
  private static final int ASSERTION_WAITING_TIME_MS = 20 * 1000;
  // waiting time when job without assertion
  private static final int RESULT_WAITING_TIME_MS = 5 * 1000;
  private static final int RESULT_CHECK_INTERVAL_MS = 500;

  private final JStormRunner stormRunner;
  private final JStormPipelineOptions options;

  private TestJStormRunner(JStormPipelineOptions options) {
    this.options = options;
    Map conf = Maps.newHashMap();
    // Default state backend is RocksDB, for the users who could not run RocksDB on local testing
    // env, following config is used to configure state backend to memory.
    // conf.put(ConfigExtension.KV_STORE_TYPE, KvStoreManagerFactory.KvStoreType.memory.toString());
    options.setTopologyConfig(conf);
    options.setLocalMode(true);
    stormRunner = JStormRunner.fromOptions(checkNotNull(options, "options"));
  }

  @Override
  public JStormRunnerResult run(Pipeline pipeline) {
    TaskReportErrorAndDie.setExceptionRecord(null);
    JStormRunnerResult result = stormRunner.run(pipeline);

    try {
      int numberOfAssertions = PAssert.countAsserts(pipeline);

      LOG.info("Running JStorm job {} with {} expected assertions.",
               result.getTopologyName(), numberOfAssertions);
      if (numberOfAssertions == 0) {
        result.waitUntilFinish(Duration.millis(RESULT_WAITING_TIME_MS));
        Exception taskExceptionRec = TaskReportErrorAndDie.getExceptionRecord();
        if (taskExceptionRec != null) {
          LOG.info("Exception was found.", taskExceptionRec);
          throw new RuntimeException(taskExceptionRec.getCause());
        }
        return result;
      } else {
        for (int waitTime = 0; waitTime <= ASSERTION_WAITING_TIME_MS;) {
          Optional<Boolean> success = checkForPAssertSuccess(result.metrics(), numberOfAssertions);
          Exception taskExceptionRec = TaskReportErrorAndDie.getExceptionRecord();
          if (success.isPresent() && success.get()) {
            return result;
          } else if (success.isPresent() && !success.get()) {
            throw new AssertionError("Failed assertion checks.");
          } else if (taskExceptionRec != null) {
            LOG.info("Exception was found.", taskExceptionRec);
            throw new RuntimeException(taskExceptionRec.getCause());
          } else {
            JStormUtils.sleepMs(RESULT_CHECK_INTERVAL_MS);
            waitTime += RESULT_CHECK_INTERVAL_MS;
          }
        }
        LOG.info("Assertion checks timed out.");
        throw new AssertionError("Assertion checks timed out.");
      }
    } finally {
      clearPAssertCount();
      cancel(result);
      TaskReportErrorAndDie.setExceptionRecord(null);
    }
  }

  private Optional<Boolean> checkForPAssertSuccess(
      MetricResults metricResults,
      int expectedNumberOfAssertions) {
    Iterable<MetricResult<Long>> successCounterResults = metricResults
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(PAssert.class, PAssert.SUCCESS_COUNTER))
            .build())
        .counters();

    long successes = 0;
    for (MetricResult<Long> counter : successCounterResults) {
      if (counter.attempted() > 0) {
        successes++;
      }
    }

    Iterable<MetricResult<Long>> failureCounterResults = metricResults
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(PAssert.class, PAssert.FAILURE_COUNTER))
            .build())
        .counters();

    long failures = 0;
    for (MetricResult<Long> counter : failureCounterResults) {
      if (counter.attempted() > 0) {
        failures++;
      }
    }

    if (failures > 0) {
      LOG.info("Found {} success, {} failures out of {} expected assertions.",
          successes, failures, expectedNumberOfAssertions);
      return Optional.of(false);
    } else if (successes == expectedNumberOfAssertions) {
      LOG.info("Found {} success, {} failures out of {} expected assertions.",
          successes, failures, expectedNumberOfAssertions);
      return Optional.of(true);
    } else if (successes > expectedNumberOfAssertions) {
      LOG.info("Found {} success, {} failures out of {} expected assertions.",
          successes, failures, expectedNumberOfAssertions);
      return Optional.of(false);
    } else {
      LOG.info("Found {} success, {} failures out of {} expected assertions.",
          successes, failures, expectedNumberOfAssertions);
      return Optional.absent();
    }
  }

  private void clearPAssertCount() {
    String topologyName = options.getJobName();
    AsmMetricRegistry taskMetrics = JStormMetrics.getTaskMetrics();
    Iterator<Map.Entry<String, AsmMetric>> itr = taskMetrics.getMetrics().entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<String, AsmMetric> metric = itr.next();
      if (metric.getKey().contains(topologyName)) {
        itr.remove();
      }
    }
  }

  private void cancel(JStormRunnerResult result) {
    try {
      result.cancel();
    } catch (IOException e) {
      throw new RuntimeException("Failed to cancel.", e);
    }
  }
}
