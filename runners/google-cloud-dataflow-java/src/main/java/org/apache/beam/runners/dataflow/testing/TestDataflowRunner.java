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

import org.apache.beam.runners.dataflow.DataflowJobExecutionException;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private final DataflowRunner runner;
  private int expectedNumberOfAssertions = 0;

  TestDataflowRunner(TestDataflowPipelineOptions options) {
    this.options = options;
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

    TestPipelineOptions testPipelineOptions = pipeline.getOptions().as(TestPipelineOptions.class);
    final DataflowPipelineJob job;
    try {
      job = runner.run(pipeline);
    } catch (DataflowJobExecutionException ex) {
      throw new IllegalStateException("The dataflow failed.");
    }

    LOG.info("Running Dataflow job {} with {} expected assertions.",
        job.getJobId(), expectedNumberOfAssertions);

    assertThat(job, testPipelineOptions.getOnCreateMatcher());

    CancelWorkflowOnError messageHandler = new CancelWorkflowOnError(
        job, new MonitoringUtil.PrintHandler(options.getJobMessageOutput()));

    try {
      final Optional<Boolean> result;

      if (options.isStreaming()) {
        Future<Optional<Boolean>> resultFuture = options.getExecutorService().submit(
            new Callable<Optional<Boolean>>() {
          @Override
          public Optional<Boolean> call() throws Exception {
            try {
              for (;;) {
                Optional<Boolean> result = checkForSuccess(job);
                if (result.isPresent()) {
                  return result;
                }
                Thread.sleep(10000L);
              }
            } finally {
              LOG.info("Cancelling Dataflow job {}", job.getJobId());
              job.cancel();
            }
          }
        });
        State finalState = job.waitToFinish(10L, TimeUnit.MINUTES, messageHandler);
        if (finalState == null || finalState == State.RUNNING) {
          LOG.info("Dataflow job {} took longer than 10 minutes to complete, cancelling.",
              job.getJobId());
          job.cancel();
        }
        result = resultFuture.get();
      } else {
        job.waitToFinish(-1, TimeUnit.SECONDS, messageHandler);
        result = checkForSuccess(job);
      }
      if (!result.isPresent()) {
        throw new IllegalStateException(
            "The dataflow did not output a success or failure metric.");
      } else if (!result.get()) {
        throw new AssertionError(messageHandler.getErrorMessage() == null
            ? "The dataflow did not return a failure reason."
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

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (transform instanceof PAssert.OneSideInputAssert
        || transform instanceof PAssert.GroupThenAssert
        || transform instanceof PAssert.GroupThenAssertForSingleton) {
      expectedNumberOfAssertions += 1;
    }

    return runner.apply(transform, input);
  }

  Optional<Boolean> checkForSuccess(DataflowPipelineJob job)
      throws IOException {
    State state = job.getState();
    if (state == State.FAILED || state == State.CANCELLED) {
      LOG.info("The pipeline failed");
      return Optional.of(false);
    }

    JobMetrics metrics = options.getDataflowClient().projects().jobs()
        .getMetrics(job.getProjectId(), job.getJobId()).execute();

    if (metrics == null || metrics.getMetrics() == null) {
      LOG.warn("Metrics not present for Dataflow job {}.", job.getJobId());
    } else {
      int successes = 0;
      int failures = 0;
      for (MetricUpdate metric : metrics.getMetrics()) {
        if (metric.getName() == null || metric.getName().getContext() == null
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
    }

    return Optional.<Boolean>absent();
  }

  @Override
  public String toString() {
    return "TestDataflowRunner#" + options.getAppName();
  }

  /**
   * Cancels the workflow on the first error message it sees.
   *
   * <p>Creates an error message representing the concatenation of all error messages seen.
   */
  private static class CancelWorkflowOnError implements JobMessagesHandler {
    private final DataflowPipelineJob job;
    private final JobMessagesHandler messageHandler;
    private final StringBuffer errorMessage;
    private CancelWorkflowOnError(DataflowPipelineJob job, JobMessagesHandler messageHandler) {
      this.job = job;
      this.messageHandler = messageHandler;
      this.errorMessage = new StringBuffer();
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
        }
      }
      if (errorMessage.length() > 0) {
        LOG.info("Cancelling Dataflow job {}", job.getJobId());
        try {
          job.cancel();
        } catch (Exception ignore) {
          // The TestDataflowRunner will thrown an AssertionError with the job failure
          // messages.
        }
      }
    }

    private String getErrorMessage() {
      return errorMessage.toString();
    }
  }
}
