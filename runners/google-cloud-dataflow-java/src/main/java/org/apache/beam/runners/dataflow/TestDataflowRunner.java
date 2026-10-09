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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TestDataflowRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final String TENTATIVE_COUNTER = "tentative";
  static final String MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY =
      "beam.dataflow.maxConcurrentStandaloneJobs";
  private static final int DEFAULT_MAX_CONCURRENT_STANDALONE_JOBS = 12;

  /**
   * Per-JVM cap on concurrently running standalone jobs, keyed by permit count rather than held as
   * a single memoized semaphore so that the cap, a system property set once per test JVM by Gradle,
   * can still be changed by tests that exercise the limit without affecting earlier callers. In a
   * real run the property never changes, so exactly one entry is ever created.
   */
  private static final ConcurrentHashMap<Integer, Semaphore> STANDALONE_SEMAPHORES =
      new ConcurrentHashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(TestDataflowRunner.class);

  /**
   * Backoff for job submissions rejected by the service for quota (HTTP 429). Many test JVMs share
   * the project's job-creation quota with no coordination between them, so a rejection usually just
   * means a brief burst; waiting it out beats failing the test. Roughly 30s, 1m, 2m, 2m, 2m (with
   * jitter), i.e. up to five retries over about eight minutes.
   */
  private static final FluentBackoff SUBMISSION_BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(Duration.standardSeconds(30))
          .withMaxBackoff(Duration.standardMinutes(2))
          .withMaxRetries(5);

  private final TestDataflowPipelineOptions options;
  private final DataflowClient dataflowClient;
  private final DataflowRunner runner;
  private final DataflowTestBatchCoordinator batchCoordinator;
  private Sleeper sleeper = Sleeper.DEFAULT;
  private int expectedNumberOfAssertions = 0;

  TestDataflowRunner(TestDataflowPipelineOptions options, DataflowClient client) {
    this(options, client, DataflowTestBatchCoordinator.shared());
  }

  TestDataflowRunner(
      TestDataflowPipelineOptions options,
      DataflowClient client,
      DataflowTestBatchCoordinator batchCoordinator) {
    this.options = options;
    this.dataflowClient = client;
    this.runner = DataflowRunner.fromOptions(options);
    this.batchCoordinator = batchCoordinator;
  }

  /** Constructs a runner from the provided options. */
  public static TestDataflowRunner fromOptions(PipelineOptions options) {
    TestDataflowPipelineOptions dataflowOptions = options.as(TestDataflowPipelineOptions.class);
    String tempLocation =
        FileSystems.matchNewDirectory(
                dataflowOptions.getTempRoot(), dataflowOptions.getJobName(), "output", "results")
            .toString();
    // to keep exact same behavior prior to matchNewDirectory introduced
    if (tempLocation.endsWith("/")) {
      tempLocation = tempLocation.substring(0, tempLocation.length() - 1);
    } else if (tempLocation.endsWith(File.separator)) {
      tempLocation = tempLocation.substring(0, tempLocation.length() - File.separator.length());
    }
    dataflowOptions.setTempLocation(tempLocation);
    String defaultPerJobStagingLocation =
        FileSystems.matchNewDirectory(tempLocation, "staging").toString();
    if (defaultPerJobStagingLocation.equals(dataflowOptions.getStagingLocation())) {
      dataflowOptions.setStagingLocation(
          FileSystems.matchNewDirectory(dataflowOptions.getTempRoot(), "staging").toString());
    }

    return new TestDataflowRunner(
        dataflowOptions, DataflowClient.create(options.as(DataflowPipelineOptions.class)));
  }

  @VisibleForTesting
  static TestDataflowRunner fromOptionsAndClient(
      TestDataflowPipelineOptions options, DataflowClient client) {
    return new TestDataflowRunner(options, client);
  }

  @VisibleForTesting
  static TestDataflowRunner fromOptionsAndClient(
      TestDataflowPipelineOptions options,
      DataflowClient client,
      DataflowTestBatchCoordinator batchCoordinator) {
    return new TestDataflowRunner(options, client, batchCoordinator);
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    return run(pipeline, runner);
  }

  DataflowPipelineJob run(Pipeline pipeline, DataflowRunner runner) {
    if (batchCoordinator.isEligibleForBatching(pipeline, options)) {
      // Eligibility implies the pipeline is a TestPipeline with a unique root name.
      return batchCoordinator.runInBatch((TestPipeline) pipeline, options, this, runner);
    }
    return runStandalone(pipeline, runner);
  }

  private static Semaphore getStandaloneSemaphore() {
    int maxJobs =
        Math.max(
            1,
            Integer.getInteger(
                MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY, DEFAULT_MAX_CONCURRENT_STANDALONE_JOBS));
    // Fair, so a test that has waited longest for a permit is next: with many test threads per JVM
    // an unfair semaphore can let a test starve behind a steady stream of newer arrivals.
    return STANDALONE_SEMAPHORES.computeIfAbsent(maxJobs, n -> new Semaphore(n, true));
  }

  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    this.sleeper = sleeper;
  }

  /**
   * Submits {@code pipeline} through {@code runner}, retrying with backoff while the service
   * rejects the submission for quota (HTTP 429). Any other failure propagates immediately.
   */
  DataflowPipelineJob submit(DataflowRunner runner, Pipeline pipeline) {
    BackOff backOff = BackOffAdapter.toGcpBackOff(SUBMISSION_BACKOFF_FACTORY.backoff());
    while (true) {
      try {
        return runner.run(pipeline);
      } catch (RuntimeException e) {
        GoogleJsonResponseException quota = quotaRejection(e);
        if (quota == null) {
          throw e;
        }
        boolean retry;
        try {
          retry = BackOffUtils.next(sleeper, backOff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        } catch (IOException ioe) {
          throw e;
        }
        if (!retry) {
          LOG.warn("Dataflow job submission still rejected for quota after retries; giving up.");
          throw e;
        }
        LOG.warn(
            "Dataflow job submission rejected for quota ({}); retrying.",
            quota.getDetails() != null ? quota.getDetails().getMessage() : quota.getMessage());
      }
    }
  }

  /** The HTTP 429 response behind {@code t}, if that is what caused it. */
  @VisibleForTesting
  static @Nullable GoogleJsonResponseException quotaRejection(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof GoogleJsonResponseException
          && ((GoogleJsonResponseException) cause).getStatusCode() == 429) {
        return (GoogleJsonResponseException) cause;
      }
    }
    return null;
  }

  /**
   * Runs {@code pipeline} as its own Dataflow job, holding a permit from the per-JVM standalone-job
   * limiter for its duration.
   */
  DataflowPipelineJob runStandalone(Pipeline pipeline, DataflowRunner runner) {
    Semaphore semaphore = getStandaloneSemaphore();
    boolean acquired = false;
    try {
      semaphore.acquire();
      acquired = true;
      return runStandaloneInternal(pipeline, runner);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      if (acquired) {
        semaphore.release();
      }
    }
  }

  /**
   * Fetches {@code job} with {@code JOB_VIEW_ALL}, which carries per-stage execution states and the
   * stage-to-user-step description needed to attribute stages to batch members. Returns {@code
   * null} if the service call fails.
   */
  @Nullable Job getJobWithExecutionDetails(DataflowPipelineJob job) {
    try {
      return dataflowClient.getJob(job.getJobId(), "JOB_VIEW_ALL");
    } catch (IOException e) {
      LOG.warn("Failed to get execution details for Dataflow job {}: ", job.getJobId(), e);
      return null;
    }
  }

  private DataflowPipelineJob runStandaloneInternal(Pipeline pipeline, DataflowRunner runner) {
    updatePAssertCount(pipeline);

    TestPipelineOptions testPipelineOptions = options.as(TestPipelineOptions.class);
    final DataflowPipelineJob job = submit(runner, pipeline);

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
      if (options.isBlockOnRun()) {
        jobSuccess = waitForStreamingJobTermination(job, messageHandler);
      } else {
        jobSuccess = true;
      }
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

  /**
   * Waits up to {@code timeout} for a merged test job to terminate, feeding its log messages to
   * {@code messageHandler}. Returns its terminal state, or {@code null} if the job did not
   * terminate in time or the wait was interrupted.
   *
   * <p>Unlike a standalone batch job, which this runner waits on indefinitely, a merged job holds
   * the verdict for several tests at once, so it is never allowed to block them forever.
   */
  @Nullable State waitForMergedJobTermination(
      DataflowPipelineJob job, Duration timeout, ErrorMonitorMessagesHandler messageHandler) {
    try {
      State state = job.waitUntilFinish(timeout, messageHandler);
      return state != null && state.isTerminal() ? state : null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /** An {@link ErrorMonitorMessagesHandler} for {@code job} that also logs every message. */
  static ErrorMonitorMessagesHandler errorMonitorFor(DataflowPipelineJob job) {
    return new ErrorMonitorMessagesHandler(job, new MonitoringUtil.LoggingHandler());
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
    if (!Strings.isNullOrEmpty(messageHandler.getErrorMessage())) {
      return messageHandler.getErrorMessage();
    } else {
      State state = job.getState();
      return String.format(
          "Dataflow job %s terminated in state %s but did not return a failure reason.",
          job.getJobId(),
          state == State.UNRECOGNIZED
              ? String.format("UNRECOGNIZED (%s)", job.getLatestStateString())
              : state.toString());
    }
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

    PAssertCounts counts = countPAssertCounters(metrics, step -> true);
    int successes = counts.successes;
    int failures = counts.failures;

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

  /** Totals of the tentative {@link PAssert} success and failure counters reported by a job. */
  static final class PAssertCounts {
    final int successes;
    final int failures;

    PAssertCounts(int successes, int failures) {
      this.successes = successes;
      this.failures = failures;
    }

    /** Whether no assertion failed and at least {@code expectedAssertions} succeeded. */
    boolean satisfy(int expectedAssertions) {
      return failures == 0 && successes >= expectedAssertions;
    }

    @Override
    public String toString() {
      return successes + " success, " + failures + " failures";
    }
  }

  /**
   * Sums the {@link PAssert} success and failure counters in {@code metrics}, considering only the
   * updates whose Dataflow step (the {@code step} entry of the metric context, or the empty string
   * if absent) is accepted by {@code stepFilter}. Only the tentative flavour of each counter is
   * counted so that its committed duplicate does not double count.
   */
  static PAssertCounts countPAssertCounters(JobMetrics metrics, Predicate<String> stepFilter) {
    int successes = 0;
    int failures = 0;
    List<MetricUpdate> updates = metrics.getMetrics();
    if (updates == null) {
      return new PAssertCounts(successes, failures);
    }
    for (MetricUpdate metric : updates) {
      MetricStructuredName name = metric.getName();
      if (name == null) {
        continue;
      }
      boolean isSuccess = PAssert.SUCCESS_COUNTER.equals(name.getName());
      boolean isFailure = PAssert.FAILURE_COUNTER.equals(name.getName());
      if (!isSuccess && !isFailure) {
        continue;
      }
      Map<String, String> context = name.getContext();
      if (context == null || !context.containsKey(TENTATIVE_COUNTER)) {
        continue;
      }
      String step = context.get("step");
      if (!stepFilter.test(step == null ? "" : step)) {
        continue;
      }
      int value = ((BigDecimal) metric.getScalar()).intValue();
      if (isSuccess) {
        successes += value;
      } else {
        failures += value;
      }
    }
    return new PAssertCounts(successes, failures);
  }

  @VisibleForTesting
  @Nullable JobMetrics getJobMetrics(DataflowPipelineJob job) {
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
  static class ErrorMonitorMessagesHandler implements JobMessagesHandler {
    private final DataflowPipelineJob job;
    private final JobMessagesHandler messageHandler;
    private final StringBuilder errorMessage;
    private volatile boolean hasSeenError;

    ErrorMonitorMessagesHandler(DataflowPipelineJob job, JobMessagesHandler messageHandler) {
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

  /**
   * Polls a job and cancels it as soon as {@code messageHandler} has seen an error. Used for
   * streaming jobs, where a failing step retries forever instead of failing the job.
   */
  static class CancelOnError implements Callable<Void> {

    private final DataflowPipelineJob job;
    private final ErrorMonitorMessagesHandler messageHandler;

    CancelOnError(DataflowPipelineJob job, ErrorMonitorMessagesHandler messageHandler) {
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
