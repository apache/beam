/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowJobExecutionException;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil.JobMessagesHandler;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * {@link TestDataflowPipelineRunner} is a pipeline runner that wraps a
 * {@link DataflowPipelineRunner} when running tests against the {@link TestPipeline}.
 *
 * @see TestPipeline
 */
public class TestDataflowPipelineRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final String TENTATIVE_COUNTER = "tentative";
  private static final Logger LOG = LoggerFactory.getLogger(TestDataflowPipelineRunner.class);

  private final TestDataflowPipelineOptions options;
  private final DataflowPipelineRunner runner;
  private int expectedNumberOfAssertions = 0;

  TestDataflowPipelineRunner(TestDataflowPipelineOptions options) {
    this.options = options;
    this.runner = DataflowPipelineRunner.fromOptions(options);
  }

  /**
   * Constructs a runner from the provided options.
   */
  public static TestDataflowPipelineRunner fromOptions(
      PipelineOptions options) {
    TestDataflowPipelineOptions dataflowOptions = options.as(TestDataflowPipelineOptions.class);

    return new TestDataflowPipelineRunner(dataflowOptions);
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    return run(pipeline, runner);
  }

  DataflowPipelineJob run(Pipeline pipeline, DataflowPipelineRunner runner) {

    final JobMessagesHandler messageHandler =
        new MonitoringUtil.PrintHandler(options.getJobMessageOutput());
    final DataflowPipelineJob job;
    try {
      job = runner.run(pipeline);
    } catch (DataflowJobExecutionException ex) {
      throw new IllegalStateException("The dataflow failed.");
    }

    LOG.info("Running Dataflow job {} with {} expected assertions.",
        job.getJobId(), expectedNumberOfAssertions);

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
        State finalState = job.waitToFinish(10L, TimeUnit.MINUTES, new JobMessagesHandler() {
            @Override
            public void process(List<JobMessage> messages) {
              messageHandler.process(messages);
              for (JobMessage message : messages) {
                if (message.getMessageImportance() != null
                    && message.getMessageImportance().equals("JOB_MESSAGE_ERROR")) {
                  LOG.info("Dataflow job {} threw exception, cancelling. Exception was: {}",
                      job.getJobId(), message.getMessageText());
                  try {
                    job.cancel();
                  } catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
            }
          });
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
        throw new IllegalStateException("The dataflow failed.");
      }
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw Throwables.propagate(e);
    }
    return job;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (transform instanceof DataflowAssert.OneSideInputAssert
        || transform instanceof DataflowAssert.TwoSideInputAssert) {
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

    JobMetrics metrics = job.getDataflowClient().projects().jobs()
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
        if (DataflowAssert.SUCCESS_COUNTER.equals(metric.getName().getName())) {
          successes += ((BigDecimal) metric.getScalar()).intValue();
        } else if (DataflowAssert.FAILURE_COUNTER.equals(metric.getName().getName())) {
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
    return "TestDataflowPipelineRunner#" + options.getAppName();
  }
}
