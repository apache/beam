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
package org.apache.beam.runners.dataflow.testing;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TestDataflowRunner} is a pipeline runner that wraps a
 * {@link DataflowRunner} when running tests against the {@link TestPipeline}.
 *
 * @see TestPipeline
 */
public class TestDataflowRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final String TENTATIVE_COUNTER = "tentative";
  // See https://issues.apache.org/jira/browse/BEAM-1170
  // we need to either fix the API or pipe the DRAINED signal through
  @VisibleForTesting
  static final String LEGACY_WATERMARK_METRIC_SUFFIX = "windmill-data-watermark";
  @VisibleForTesting
  static final String WATERMARK_METRIC_SUFFIX = "DataWatermark";
  private static final long MAX_WATERMARK_VALUE = -2L;
  private static final Logger LOG = LoggerFactory.getLogger(TestDataflowRunner.class);

  private final TestDataflowPipelineOptions options;
  private final DataflowClient dataflowClient;
  private final DataflowRunner runner;
  private int expectedNumberOfAssertions = 0;

  TestDataflowRunner(TestDataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = DataflowClient.create(options);
    this.runner = DataflowRunner.fromOptions(options);
  }

  /**
   * Constructs a runner from the provided options.
   */
  public static TestDataflowRunner fromOptions(PipelineOptions options) {
    TestDataflowPipelineOptions dataflowOptions = options.as(TestDataflowPipelineOptions.class);
    String tempLocation = Joiner.on("/").join(
        dataflowOptions.getTempRoot(),
        dataflowOptions.getJobName(),
        "output",
        "results");
    dataflowOptions.setTempLocation(tempLocation);

    return new TestDataflowRunner(dataflowOptions);
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    return run(pipeline, runner);
  }

  DataflowPipelineJob run(Pipeline pipeline, DataflowRunner runner) {
    updatePAssertCount(pipeline);

    TestPipelineOptions testPipelineOptions = pipeline.getOptions().as(TestPipelineOptions.class);
    final DataflowPipelineJob job;
    job = runner.run(pipeline);

    LOG.info("Running Dataflow job {} with {} expected assertions.",
        job.getJobId(), expectedNumberOfAssertions);

    assertThat(job, testPipelineOptions.getOnCreateMatcher());

    final ErrorMonitorMessagesHandler messageHandler =
        new ErrorMonitorMessagesHandler(job, new MonitoringUtil.LoggingHandler());

    try {
      final Optional<Boolean> success;

      if (options.isStreaming()) {
        Future<Optional<Boolean>> resultFuture = options.getExecutorService().submit(
            new Callable<Optional<Boolean>>() {
          @Override
          public Optional<Boolean> call() throws Exception {
            try {
              for (;;) {
                JobMetrics metrics = getJobMetrics(job);
                Optional<Boolean> success = checkForPAssertSuccess(job, metrics);
                if (messageHandler.hasSeenError()) {
                  return Optional.of(false);
                }

                if (success.isPresent() && (!success.get() || atMaxWatermark(job, metrics))) {
                  // It's possible that the streaming pipeline doesn't use PAssert.
                  // So checkForSuccess() will return true before job is finished.
                  // atMaxWatermark() will handle this case.
                  return success;
                }
                Thread.sleep(10000L);
              }
            } finally {
              if (!job.getState().isTerminal()) {
                LOG.info("Cancelling Dataflow job {}", job.getJobId());
                job.cancel();
              }
            }
          }
        });
        State finalState =
            job.waitUntilFinish(
                Duration.standardSeconds(options.getTestTimeoutSeconds()), messageHandler);
        if (finalState == null || finalState == State.RUNNING) {
          LOG.info(
              "Dataflow job {} took longer than {} seconds to complete, cancelling.",
              job.getJobId(),
              options.getTestTimeoutSeconds());
          job.cancel();
        }
        success = resultFuture.get();
      } else {
        job.waitUntilFinish(Duration.standardSeconds(-1), messageHandler);
        success = checkForPAssertSuccess(job, getJobMetrics(job));
      }
      if (!success.isPresent()) {
        throw new IllegalStateException(
            "The dataflow did not output a success or failure metric.");
      } else if (!success.get()) {
        throw new AssertionError(
            Strings.isNullOrEmpty(messageHandler.getErrorMessage())
                ? String.format(
                    "Dataflow job %s terminated in state %s but did not return a failure reason.",
                    job.getJobId(), job.getState())
                : messageHandler.getErrorMessage());
      } else {
        assertThat(job, testPipelineOptions.getOnSuccessMatcher());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return job;
  }

  @VisibleForTesting
  void updatePAssertCount(Pipeline pipeline) {
    DataflowPipelineOptions options = pipeline.getOptions().as(DataflowPipelineOptions.class);
    if (DataflowRunner.hasExperiment(options, "beam_fn_api")) {
      // TODO[BEAM-1866]: FnAPI does not support metrics, so expect 0 assertions.
      expectedNumberOfAssertions = 0;
    } else {
      expectedNumberOfAssertions = PAssert.countAsserts(pipeline);
    }
  }

  /**
   * Check that PAssert expectations were met.
   *
   * <p>If the pipeline is not in a failed/cancelled state and no PAsserts were used
   * within the pipeline, then this method will state that all PAsserts succeeded.
   *
   * @return Optional.of(false) if the job failed, was cancelled or if any PAssert
   *         expectation was not met, true if all the PAssert expectations passed,
   *         Optional.absent() if the metrics were inconclusive.
   */
  @VisibleForTesting
  Optional<Boolean> checkForPAssertSuccess(DataflowPipelineJob job, @Nullable JobMetrics metrics)
      throws IOException {
    State state = job.getState();
    if (state == State.FAILED || state == State.CANCELLED) {
      LOG.info("The pipeline failed");
      return Optional.of(false);
    }

    if (metrics == null || metrics.getMetrics() == null) {
      LOG.warn("Metrics not present for Dataflow job {}.", job.getJobId());
      return Optional.absent();
    }

    int successes = 0;
    int failures = 0;
    for (MetricUpdate metric : metrics.getMetrics()) {
      if (metric.getName() == null
          || metric.getName().getContext() == null
          || !metric.getName().getContext().containsKey(TENTATIVE_COUNTER)) {
        // Don't double count using the non-tentative version of the metric.
        continue;
      }
      if (PAssert.SUCCESS_COUNTER.equals(metric.getName().getName())) {
        successes += ((BigDecimal) metric.getScalar()).intValue();
      } else if (PAssert.FAILURE_COUNTER.equals(metric.getName().getName())) {
        failures += ((BigDecimal) metric.getScalar()).intValue();
      }
    }

    if (failures > 0) {
      LOG.info("Found result while running Dataflow job {}. Found {} success, {} failures out of "
          + "{} expected assertions.", job.getJobId(), successes, failures,
          expectedNumberOfAssertions);
      return Optional.of(false);
    } else if (successes >= expectedNumberOfAssertions) {
      LOG.info("Found result while running Dataflow job {}. Found {} success, {} failures out of "
          + "{} expected assertions.", job.getJobId(), successes, failures,
          expectedNumberOfAssertions);
      return Optional.of(true);
    }

    LOG.info("Running Dataflow job {}. Found {} success, {} failures out of {} expected "
        + "assertions.", job.getJobId(), successes, failures, expectedNumberOfAssertions);
    return Optional.absent();
  }

  /**
   * Checks wether a metric is a streaming watermark.
   *
   * @return true if the metric is a watermark.
   */
  boolean isWatermark(MetricUpdate metric) {
    if (metric.getName() == null || metric.getName().getName() == null) {
      return false; // no name -> shouldn't happen, not the watermark
    }
    if (metric.getScalar() == null) {
      return false; // no scalar value -> not the watermark
    }
    String name = metric.getName().getName();
    return name.endsWith(LEGACY_WATERMARK_METRIC_SUFFIX)
        || name.endsWith(WATERMARK_METRIC_SUFFIX);
  }

  /**
   * Check watermarks of the streaming job. At least one watermark metric must exist.
   *
   * @return true if all watermarks are at max, false otherwise.
   */
  @VisibleForTesting
  boolean atMaxWatermark(DataflowPipelineJob job, JobMetrics metrics) {
    boolean hasMaxWatermark = false;
    for (MetricUpdate metric : metrics.getMetrics()) {
      if (!isWatermark(metric)) {
        continue;
      }
      BigDecimal watermark = (BigDecimal) metric.getScalar();
      hasMaxWatermark = watermark.longValue() == MAX_WATERMARK_VALUE;
      if (!hasMaxWatermark) {
        LOG.info("Found a non-max watermark metric {} in job {}", metric.getName().getName(),
            job.getJobId());
        return false;
      }
    }

    if (hasMaxWatermark) {
      LOG.info("All watermarks are at max. JobID: {}", job.getJobId());
    }
    return hasMaxWatermark;
  }

  @Nullable
  @VisibleForTesting
  JobMetrics getJobMetrics(DataflowPipelineJob job) {
    JobMetrics metrics = null;
    try {
      metrics = dataflowClient.getJobMetrics(job.getJobId());
    } catch (IOException e) {
      LOG.warn("Failed to get job metrics: ", e);
    }
    return metrics;
  }

  @Override
  public String toString() {
    return "TestDataflowRunner#" + options.getAppName();
  }

  /**
   * Monitors job log output messages for errors.
   *
   * <p>Creates an error message representing the concatenation of all error messages seen.
   */
  private static class ErrorMonitorMessagesHandler implements JobMessagesHandler {
    private final DataflowPipelineJob job;
    private final JobMessagesHandler messageHandler;
    private final StringBuffer errorMessage;
    private volatile boolean hasSeenError;

    private ErrorMonitorMessagesHandler(
        DataflowPipelineJob job, JobMessagesHandler messageHandler) {
      this.job = job;
      this.messageHandler = messageHandler;
      this.errorMessage = new StringBuffer();
      this.hasSeenError = false;
    }

    @Override
    public void process(List<JobMessage> messages) {
      messageHandler.process(messages);
      for (JobMessage message : messages) {
        if (message.getMessageImportance() != null
            && message.getMessageImportance().equals("JOB_MESSAGE_ERROR")) {
          LOG.info("Dataflow job {} threw exception. Failure message was: {}",
              job.getJobId(), message.getMessageText());
          errorMessage.append(message.getMessageText());
          hasSeenError = true;
        }
      }
    }

    boolean hasSeenError() {
      return hasSeenError;
    }

    String getErrorMessage() {
      return errorMessage.toString();
    }
  }
}
