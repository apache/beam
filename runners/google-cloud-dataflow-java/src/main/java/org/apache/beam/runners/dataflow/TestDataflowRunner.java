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

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TestDataflowRunner} is a pipeline runner that wraps a {@link DataflowRunner} when running
 * tests against the {@link TestPipeline}.
 *
 * @see TestPipeline
 */
public class TestDataflowRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final String TENTATIVE_COUNTER = "tentative";
  private static final Logger LOG = LoggerFactory.getLogger(TestDataflowRunner.class);

  private final TestDataflowPipelineOptions options;
  private final DataflowClient dataflowClient;
  private final DataflowRunner runner;
  private int expectedNumberOfAssertions = 0;

  TestDataflowRunner(TestDataflowPipelineOptions options, DataflowClient client) {
    this.options = options;
    this.dataflowClient = client;
    this.runner = DataflowRunner.fromOptions(options);
  }

  /** Constructs a runner from the provided options. */
  public static TestDataflowRunner fromOptions(PipelineOptions options) {
    TestDataflowPipelineOptions dataflowOptions = options.as(TestDataflowPipelineOptions.class);
    String tempLocation =
        Joiner.on("/")
            .join(dataflowOptions.getTempRoot(), dataflowOptions.getJobName(), "output", "results");
    dataflowOptions.setTempLocation(tempLocation);

    return new TestDataflowRunner(
        dataflowOptions, DataflowClient.create(options.as(DataflowPipelineOptions.class)));
  }

  @VisibleForTesting
  static TestDataflowRunner fromOptionsAndClient(
      TestDataflowPipelineOptions options, DataflowClient client) {
    return new TestDataflowRunner(options, client);
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    return run(pipeline, runner);
  }

  DataflowPipelineJob run(Pipeline pipeline, DataflowRunner runner) {
    updatePAssertCount(pipeline);

    TestPipelineOptions testPipelineOptions = options.as(TestPipelineOptions.class);
    final DataflowPipelineJob job;
    job = runner.run(pipeline);

    LOG.info(
        "Running Dataflow job {} with {} expected assertions.",
        job.getJobId(),
        expectedNumberOfAssertions);

    assertThat(job, testPipelineOptions.getOnCreateMatcher());

    Boolean jobSuccess;
    Optional<Boolean> allAssertionsPassed;

    ErrorMonitorMessagesHandler messageHandler =
        new ErrorMonitorMessagesHandler(job, new MonitoringUtil.LoggingHandler());

    if (options.isStreaming()) {
      jobSuccess = waitForStreamingJobTermination(job, messageHandler);
      // No metrics in streaming
      allAssertionsPassed = Optional.absent();
    } else {
      jobSuccess = waitForBatchJobTermination(job, messageHandler);
      allAssertionsPassed = checkForPAssertSuccess(job);
    }

    // If there is a certain assertion failure, throw the most precise exception we can.
    // There are situations where the metric will not be available, but as long as we recover
    // the actionable message from the logs it is acceptable.
    if (!allAssertionsPassed.isPresent()) {
      LOG.warn("Dataflow job {} did not output a success or failure metric.", job.getJobId());
    } else if (!allAssertionsPassed.get()) {
      throw new AssertionError(errorMessage(job, messageHandler));
    }

    // Other failures, or jobs where metrics fell through for some reason, will manifest
    // as simply job failures.
    if (!jobSuccess) {
      throw new RuntimeException(errorMessage(job, messageHandler));
    }

    // If there is no reason to immediately fail, run the success matcher.
    assertThat(job, testPipelineOptions.getOnSuccessMatcher());
    return job;
  }

  /**
   * Return {@code true} if the job succeeded or {@code false} if it terminated in any other manner.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // Job status checked via job.waitUntilFinish
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  private boolean waitForStreamingJobTermination(
      final DataflowPipelineJob job, ErrorMonitorMessagesHandler messageHandler) {
    // In streaming, there are infinite retries, so rather than timeout
    // we try to terminate early by polling and canceling if we see
    // an error message
    options.getExecutorService().submit(new CancelOnError(job, messageHandler));

    // Whether we canceled or not, this gets the final state of the job or times out
    State finalState;
    try {
      finalState =
          job.waitUntilFinish(
              Duration.standardSeconds(options.getTestTimeoutSeconds()), messageHandler);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      return false;
    }

    // Getting the final state may have timed out; it may not indicate a failure.
    // This cancellation may be the second
    if (finalState == null || !finalState.isTerminal()) {
      LOG.info(
          "Dataflow job {} took longer than {} seconds to complete, cancelling.",
          job.getJobId(),
          options.getTestTimeoutSeconds());
      try {
        job.cancel();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return false;
    } else {
      return finalState == State.DONE && !messageHandler.hasSeenError();
    }
  }

  /** Return {@code true} if job state is {@code State.DONE}. {@code false} otherwise. */
  private boolean waitForBatchJobTermination(
      DataflowPipelineJob job, ErrorMonitorMessagesHandler messageHandler) {
    {
      try {
        job.waitUntilFinish(Duration.standardSeconds(-1), messageHandler);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.interrupted();
        return false;
      }

      return job.getState() == State.DONE;
    }
  }

  private static String errorMessage(
      DataflowPipelineJob job, ErrorMonitorMessagesHandler messageHandler) {
    return Strings.isNullOrEmpty(messageHandler.getErrorMessage())
        ? String.format(
            "Dataflow job %s terminated in state %s but did not return a failure reason.",
            job.getJobId(), job.getState())
        : messageHandler.getErrorMessage();
  }

  @VisibleForTesting
  void updatePAssertCount(Pipeline pipeline) {
    expectedNumberOfAssertions = PAssert.countAsserts(pipeline);
  }

  /**
   * Check that PAssert expectations were met.
   *
   * <p>If the pipeline is not in a failed/cancelled state and no PAsserts were used within the
   * pipeline, then this method will state that all PAsserts succeeded.
   *
   * @return Optional.of(false) if we are certain a PAssert failed. Optional.of(true) if we are
   *     certain all PAsserts passed. Optional.absent() if the evidence is inconclusive, including
   *     when the pipeline may have failed for other reasons.
   */
  @VisibleForTesting
  Optional<Boolean> checkForPAssertSuccess(DataflowPipelineJob job) {

    JobMetrics metrics = getJobMetrics(job);
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
      LOG.info(
          "Failure result for Dataflow job {}. Found {} success, {} failures out of "
              + "{} expected assertions.",
          job.getJobId(),
          successes,
          failures,
          expectedNumberOfAssertions);
      return Optional.of(false);
    } else if (successes >= expectedNumberOfAssertions) {
      LOG.info(
          "Success result for Dataflow job {}."
              + " Found {} success, {} failures out of {} expected assertions.",
          job.getJobId(),
          successes,
          failures,
          expectedNumberOfAssertions);
      return Optional.of(true);
    }

    // If the job failed, this is a definite failure. We only cancel jobs when they fail.
    State state = job.getState();
    if (state == State.FAILED || state == State.CANCELLED) {
      LOG.info(
          "Dataflow job {} terminated in failure state {} without reporting a failed assertion",
          job.getJobId(),
          state);
      return Optional.absent();
    }

    LOG.info(
        "Inconclusive results for Dataflow job {}."
            + " Found {} success, {} failures out of {} expected assertions.",
        job.getJobId(),
        successes,
        failures,
        expectedNumberOfAssertions);
    return Optional.absent();
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
    private final StringBuilder errorMessage;
    private volatile boolean hasSeenError;

    private ErrorMonitorMessagesHandler(
        DataflowPipelineJob job, JobMessagesHandler messageHandler) {
      this.job = job;
      this.messageHandler = messageHandler;
      this.errorMessage = new StringBuilder();
      this.hasSeenError = false;
    }

    @Override
    public void process(List<JobMessage> messages) {
      messageHandler.process(messages);
      for (JobMessage message : messages) {
        if ("JOB_MESSAGE_ERROR".equals(message.getMessageImportance())) {
          LOG.info(
              "Dataflow job {} threw exception. Failure message was: {}",
              job.getJobId(),
              message.getMessageText());
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

  private static class CancelOnError implements Callable<Void> {

    private final DataflowPipelineJob job;
    private final ErrorMonitorMessagesHandler messageHandler;

    public CancelOnError(DataflowPipelineJob job, ErrorMonitorMessagesHandler messageHandler) {
      this.job = job;
      this.messageHandler = messageHandler;
    }

    @Override
    public Void call() throws Exception {
      while (true) {
        State jobState = job.getState();

        // If we see an error, cancel and note failure
        if (messageHandler.hasSeenError() && !job.getState().isTerminal()) {
          job.cancel();
          LOG.info("Cancelling Dataflow job {}", job.getJobId());
          return null;
        }

        if (jobState.isTerminal()) {
          return null;
        }

        Thread.sleep(3000L);
      }
    }
  }
}
