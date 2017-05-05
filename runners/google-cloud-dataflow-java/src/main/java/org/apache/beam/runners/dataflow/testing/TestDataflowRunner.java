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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
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

    LOG.info("Running Dataflow job {} with {} expected assertions.",
        job.getJobId(), expectedNumberOfAssertions);

    assertThat(job, testPipelineOptions.getOnCreateMatcher());

    final ErrorMonitorMessagesHandler messageHandler =
        new ErrorMonitorMessagesHandler(job, new MonitoringUtil.LoggingHandler());

    try {
      Optional<Boolean> result = Optional.absent();

      if (options.isStreaming()) {
        // In streaming, there are infinite retries, so rather than timeout
        // we try to terminate early by polling and canceling if we see
        // an error message
        while (true) {
          State state = job.waitUntilFinish(Duration.standardSeconds(3), messageHandler);
          if (state != null && state.isTerminal()) {
            break;
          }

          if (messageHandler.hasSeenError()) {
            if (!job.getState().isTerminal()) {
              LOG.info("Cancelling Dataflow job {}", job.getJobId());
              job.cancel();
            }
            break;
          }
        }

        // Whether we canceled or not, this gets the final state of the job or times out
        State finalState =
            job.waitUntilFinish(
                Duration.standardSeconds(options.getTestTimeoutSeconds()), messageHandler);

        // Getting the final state timed out; it may not indicate a failure.
        // This cancellation may be the second
        if (finalState == null || finalState == State.RUNNING) {
          LOG.info(
              "Dataflow job {} took longer than {} seconds to complete, cancelling.",
              job.getJobId(),
              options.getTestTimeoutSeconds());
          job.cancel();
        }

        if (messageHandler.hasSeenError()) {
          result = Optional.of(false);
        }
      } else {
        job.waitUntilFinish(Duration.standardSeconds(-1), messageHandler);
        result = checkForPAssertSuccess(job);
      }

      if (!result.isPresent()) {
        if (options.isStreaming()) {
          LOG.warn(
              "Dataflow job {} did not output a success or failure metric."
                  + " In rare situations, some PAsserts may not have run."
                  + " This is a known limitation of Dataflow in streaming.",
              job.getJobId());
        } else {
          throw new IllegalStateException(
              String.format(
                  "Dataflow job %s did not output a success or failure metric.", job.getJobId()));
        }
      } else if (!result.get()) {
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return job;
  }

  @VisibleForTesting
  void updatePAssertCount(Pipeline pipeline) {
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
   * <p>If the pipeline is not in a failed/cancelled state and no PAsserts were used within the
   * pipeline, then this method will state that all PAsserts succeeded.
   *
   * @return Optional.of(false) if we are certain a PAssert or some other critical thing has failed,
   *     Optional.of(true) if we are certain all PAsserts passed, and Optional.absent() if the
   *     evidence is inconclusive.
   */
  @VisibleForTesting
  Optional<Boolean> checkForPAssertSuccess(DataflowPipelineJob job) throws IOException {

    // If the job failed, this is a definite failure. We only cancel jobs when they fail.
    State state = job.getState();
    if (state == State.FAILED || state == State.CANCELLED) {
      LOG.info("Dataflow job {} terminated in failure state {}", job.getJobId(), state);
      return Optional.of(false);
    }

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
      LOG.info("Failure result for Dataflow job {}. Found {} success, {} failures out of "
          + "{} expected assertions.", job.getJobId(), successes, failures,
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
