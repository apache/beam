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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.dataflow.model.ComponentSource;
import com.google.api.services.dataflow.model.ComponentTransform;
import com.google.api.services.dataflow.model.ExecutionStageState;
import com.google.api.services.dataflow.model.ExecutionStageSummary;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.StageSource;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatchers;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates merging concurrent batch {@code TestPipeline} executions within a JVM into a single
 * Dataflow job per batch, with scoped {@link PAssert} and {@link MetricResults} verification.
 *
 * <p>Threading model: a test thread only enqueues its pipeline and then blocks on a per-pipeline
 * future. Per {@linkplain #compatibilityKey options-compatibility group}, a daemon scheduler thread
 * drains the queue and groups pipelines into batches; each closed batch is handed to a daemon
 * executor that submits the merged job, waits for it and reaches a verdict for every member. A
 * member is credited from the merged job when its scoped PAsserts are verified and &mdash; if the
 * job did not end {@code DONE} &mdash; every stage that may belong to it is {@code DONE} (see
 * {@link StageAttribution}). Any other member is told to run standalone, which it does on its own
 * test thread. No test thread ever performs work on behalf of another test, so a test timeout or
 * interrupt affects only that test.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class DataflowTestBatchCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowTestBatchCoordinator.class);

  static final String TEST_BATCHING_PROPERTY = "beam.dataflow.testBatching";
  static final String TEST_BATCH_MAX_SIZE_PROPERTY = "beam.dataflow.testBatchMaxSize";
  static final String TEST_BATCH_WINDOW_MS_PROPERTY = "beam.dataflow.testBatchWindowMs";
  private static final String VALIDATES_RUNNER_THREADS_PROPERTY =
      "beam.validatesRunner.parallelThreads";

  private static final long DEFAULT_BATCH_WINDOW_MS = 3000L;
  private static final int DEFAULT_BATCH_MAX_SIZE = 30;
  private static final String TENTATIVE_COUNTER = "tentative";

  /**
   * Run-wide batching configuration. Batching spans many pipelines, so it is configured once per
   * test JVM through system properties (see the ValidatesRunner tasks in {@code build.gradle})
   * rather than through any individual pipeline's {@link PipelineOptions}.
   */
  @VisibleForTesting
  static final class BatchingConfig {
    final boolean enabled;
    final int maxBatchSize;
    final long windowMs;

    BatchingConfig(boolean enabled, int maxBatchSize, long windowMs) {
      this.enabled = enabled;
      this.maxBatchSize = maxBatchSize;
      this.windowMs = windowMs;
    }

    static BatchingConfig fromSystemProperties() {
      int defaultSize =
          Integer.getInteger(VALIDATES_RUNNER_THREADS_PROPERTY, DEFAULT_BATCH_MAX_SIZE);
      return new BatchingConfig(
          Boolean.getBoolean(TEST_BATCHING_PROPERTY),
          Integer.getInteger(TEST_BATCH_MAX_SIZE_PROPERTY, Math.max(1, defaultSize)),
          Long.getLong(TEST_BATCH_WINDOW_MS_PROPERTY, DEFAULT_BATCH_WINDOW_MS));
    }
  }

  private static volatile BatchingConfig config = BatchingConfig.fromSystemProperties();

  /** Overrides the run-wide configuration; {@code null} re-reads the system properties. */
  @VisibleForTesting
  static void setConfigForTesting(@Nullable BatchingConfig override) {
    config = override == null ? BatchingConfig.fromSystemProperties() : override;
  }

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /**
   * Options that are expected to differ between tests which may nevertheless share a merged job.
   *
   * <p>The merged job is submitted with the <em>first</em> member's options only, so every other
   * option that has been set on a member must be identical across the batch. Keep this list to
   * per-test identity and local harness knobs; never add anything that influences how the service
   * runs the job.
   */
  @VisibleForTesting
  static final ImmutableSet<String> PER_TEST_OPTIONS =
      ImmutableSet.of(
          // Identity of the individual test / job.
          "jobName",
          "appName",
          "optionsId",
          // Derived per job from tempRoot + jobName by TestDataflowRunner.fromOptions.
          "tempLocation",
          "gcpTempLocation",
          // Local harness behaviour; does not affect the submitted job.
          "testTimeoutSeconds");

  private static final AtomicInteger BATCH_COUNTER = new AtomicInteger(1);
  private static final AtomicInteger COLLECTOR_COUNTER = new AtomicInteger(1);
  private static final ConcurrentHashMap<String, BatchCollector> COLLECTORS =
      new ConcurrentHashMap<>();

  /**
   * Returns a key identifying the group of pipelines that may share a merged job with one built
   * from {@code options}, or {@code null} if the options cannot be serialized, in which case the
   * pipeline must run standalone.
   *
   * <p>The key is a digest of every option that has been set on (or lazily bound to) the options
   * object through <em>any</em> {@link PipelineOptions} interface it has been viewed as, minus
   * {@link #PER_TEST_OPTIONS}. Deriving it from the serialized form rather than from a hand-picked
   * list of getters means that an option added to any interface (worker pool, debug, GCP, SDK
   * harness, user-defined, ...) automatically splits batches when it differs instead of being
   * silently dropped for all but the first member.
   */
  @VisibleForTesting
  static @Nullable String compatibilityKey(PipelineOptions options) {
    String canonical = canonicalOptionsJson(options);
    return canonical == null ? null : digest(canonical);
  }

  private static String digest(String canonicalOptions) {
    return Hashing.sha256().hashString(canonicalOptions, StandardCharsets.UTF_8).toString();
  }

  @VisibleForTesting
  static @Nullable String canonicalOptionsJson(PipelineOptions options) {
    JsonNode serialized;
    try {
      serialized = MAPPER.valueToTree(options).get("options");
    } catch (RuntimeException e) {
      LOG.warn(
          "Pipeline options for {} cannot be serialized; the pipeline will run standalone instead"
              + " of in a merged Dataflow test job.",
          options.as(ApplicationNameOptions.class).getAppName(),
          e);
      return null;
    }
    if (serialized == null || !serialized.isObject()) {
      return null;
    }
    ObjectNode filtered = MAPPER.createObjectNode();
    for (Map.Entry<String, JsonNode> entry : sortedFields(serialized).entrySet()) {
      if (!PER_TEST_OPTIONS.contains(entry.getKey())) {
        filtered.set(entry.getKey(), canonicalize(entry.getValue()));
      }
    }
    return filtered.toString();
  }

  /** Recursively orders object fields so that structurally equal values serialize identically. */
  private static JsonNode canonicalize(JsonNode node) {
    if (node.isObject()) {
      ObjectNode out = MAPPER.createObjectNode();
      for (Map.Entry<String, JsonNode> entry : sortedFields(node).entrySet()) {
        out.set(entry.getKey(), canonicalize(entry.getValue()));
      }
      return out;
    }
    if (node.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode element : node) {
        out.add(canonicalize(element));
      }
      return out;
    }
    return node;
  }

  private static TreeMap<String, JsonNode> sortedFields(JsonNode node) {
    TreeMap<String, JsonNode> sorted = new TreeMap<>();
    node.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), entry.getValue()));
    return sorted;
  }

  static boolean isEligibleForBatching(Pipeline pipeline, TestDataflowPipelineOptions options) {
    BatchingConfig current = config;
    if (!current.enabled || current.maxBatchSize <= 1) {
      return false;
    }
    if (options.isStreaming() || !options.isBlockOnRun()) {
      return false;
    }
    if (pipeline instanceof CompositeBatchPipeline || pipeline.isStandaloneExecutionRequired()) {
      return false;
    }
    if (!isDefaultMatcher(options.getOnCreateMatcher())
        || !isDefaultMatcher(options.getOnSuccessMatcher())) {
      return false;
    }
    EligibilityVisitor visitor = new EligibilityVisitor();
    pipeline.traverseTopologically(visitor);
    return visitor.hasPrimitiveTransform && !visitor.hasUnboundedPCollection;
  }

  private static boolean isDefaultMatcher(@Nullable Object matcher) {
    return matcher == null
        || matcher instanceof TestPipelineOptions.AlwaysPassMatcher
        || SerializableMatchers.anything().equals(matcher);
  }

  static DataflowPipelineJob runInBatch(
      Pipeline pipeline,
      TestDataflowPipelineOptions options,
      TestDataflowRunner runner,
      DataflowRunner delegateRunner) {
    final String canonicalOptions = canonicalOptionsJson(options);
    if (canonicalOptions == null) {
      return runner.runStandalone(pipeline, delegateRunner);
    }
    String key = digest(canonicalOptions);
    PendingItem item = new PendingItem(pipeline, options, runner, delegateRunner);
    BatchCollector collector =
        COLLECTORS.computeIfAbsent(
            key,
            k -> {
              BatchCollector created =
                  new BatchCollector("beam-test-batch-" + COLLECTOR_COUNTER.getAndIncrement());
              LOG.info(
                  "Created Dataflow test batch group {} ({}) for options compatible with {}",
                  created.name,
                  k,
                  options.getAppName());
              LOG.debug("Batch group {} options: {}", created.name, canonicalOptions);
              created.start();
              return created;
            });
    collector.enqueue(item);
    return item.awaitResult();
  }

  private static class EligibilityVisitor extends PipelineVisitor.Defaults {
    private boolean hasPrimitiveTransform = false;
    private boolean hasUnboundedPCollection = false;

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      hasPrimitiveTransform = true;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (value instanceof PCollection
          && ((PCollection<?>) value).isBounded() == IsBounded.UNBOUNDED) {
        hasUnboundedPCollection = true;
      }
    }
  }

  /**
   * The verdict the batch machinery reaches for one member pipeline. Either the member is credited
   * with a {@link #job} from the merged run, or it must be run standalone by its own test thread.
   */
  @VisibleForTesting
  static final class BatchOutcome {
    final @Nullable DataflowPipelineJob job;
    final boolean limitConcurrency;

    private BatchOutcome(@Nullable DataflowPipelineJob job, boolean limitConcurrency) {
      this.job = job;
      this.limitConcurrency = limitConcurrency;
    }

    static BatchOutcome passed(DataflowPipelineJob job) {
      return new BatchOutcome(job, true);
    }

    /**
     * The member must run standalone. {@code limitConcurrency} is {@code false} for re-runs after a
     * merged job was attempted: those are bounded by the batch size and must not queue behind the
     * per-JVM standalone limiter, otherwise members that have already waited for the merged job
     * could exceed their test timeout waiting for a permit.
     */
    static BatchOutcome runStandalone(boolean limitConcurrency) {
      return new BatchOutcome(null, limitConcurrency);
    }
  }

  @VisibleForTesting
  static final class PendingItem {
    final Pipeline pipeline;
    final TestDataflowPipelineOptions options;
    final TestDataflowRunner runner;
    final DataflowRunner delegateRunner;
    final int expectedAssertions;
    final Runnable restoreSnapshot;
    final CompletableFuture<BatchOutcome> resultFuture = new CompletableFuture<>();

    /** Assigned by the batch runner when the merged job is assembled; {@code null} before that. */
    @Nullable String scopePrefix;

    PendingItem(
        Pipeline pipeline,
        TestDataflowPipelineOptions options,
        TestDataflowRunner runner,
        DataflowRunner delegateRunner) {
      this.pipeline = pipeline;
      this.options = options;
      this.runner = runner;
      this.delegateRunner = delegateRunner;
      this.expectedAssertions = PAssert.countAsserts(pipeline);
      this.restoreSnapshot = pipeline.captureStateSnapshot();
    }

    /**
     * Blocks the calling (test) thread until the batch machinery has reached a verdict, then either
     * returns the merged job or runs this pipeline standalone on the calling thread.
     */
    DataflowPipelineJob awaitResult() {
      BatchOutcome outcome;
      try {
        outcome = resultFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while waiting for the merged Dataflow test batch containing "
                + options.getAppName(),
            e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new RuntimeException(cause != null ? cause : e);
      }
      // The batch runner is done with this pipeline once the future is complete, so it is safe to
      // undo the root-name prefix and any transform-override surgery performed for the merged job.
      restoreSnapshot.run();
      if (outcome.job != null) {
        return outcome.job;
      }
      return runner.runStandalone(pipeline, delegateRunner, outcome.limitConcurrency);
    }
  }

  private static ThreadFactory daemonThreadFactory(String namePrefix) {
    AtomicInteger counter = new AtomicInteger(1);
    return runnable -> {
      Thread thread = new Thread(runnable, namePrefix + counter.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * Groups pipelines that share a {@link BatchKey} into batches.
   *
   * <p>Test threads only {@link #enqueue} and then wait on their {@link PendingItem#resultFuture}.
   * A single daemon scheduler thread drains the queue: it takes the first pipeline, waits up to the
   * batch window for more (closing early once the batch is full), and hands the closed batch to a
   * daemon executor so that it can immediately start collecting the next batch while the merged job
   * is in flight. A batch of one is not worth a merged job and is sent straight back to its test
   * thread to run standalone.
   */
  private static final class BatchCollector {
    private final String name;
    private final LinkedBlockingQueue<PendingItem> queue = new LinkedBlockingQueue<>();
    private final ExecutorService batchRunners;
    private @Nullable Thread scheduler;

    BatchCollector(String name) {
      this.name = name;
      this.batchRunners =
          Executors.newCachedThreadPool(daemonThreadFactory(name + "-batch-runner-"));
    }

    synchronized void start() {
      if (scheduler == null) {
        scheduler = new Thread(this::schedulerLoop, name + "-scheduler");
        scheduler.setDaemon(true);
        scheduler.start();
      }
    }

    void enqueue(PendingItem item) {
      queue.add(item);
    }

    private void schedulerLoop() {
      while (true) {
        List<PendingItem> batch;
        try {
          batch = collectBatch(queue.take());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        dispatch(batch);
      }
      IllegalStateException stopped =
          new IllegalStateException("Dataflow test batch scheduler " + name + " was interrupted");
      List<PendingItem> orphans = new ArrayList<>();
      queue.drainTo(orphans);
      for (PendingItem orphan : orphans) {
        orphan.resultFuture.completeExceptionally(stopped);
      }
    }

    private List<PendingItem> collectBatch(PendingItem first) throws InterruptedException {
      List<PendingItem> batch = new ArrayList<>();
      batch.add(first);
      BatchingConfig current = config;
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(current.windowMs);
      while (batch.size() < current.maxBatchSize) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        PendingItem next = queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
        if (next == null) {
          break;
        }
        batch.add(next);
      }
      return batch;
    }

    private void dispatch(List<PendingItem> batch) {
      try {
        if (batch.size() == 1) {
          batch.get(0).resultFuture.complete(BatchOutcome.runStandalone(true));
          return;
        }
        batchRunners.execute(() -> executeBatch(batch));
      } catch (Throwable t) {
        for (PendingItem item : batch) {
          item.resultFuture.completeExceptionally(t);
        }
      }
    }
  }

  /**
   * Runs one merged Dataflow job for {@code items} and completes every member's future. Never
   * throws and never leaves a future incomplete: an unexpected failure anywhere in here is
   * propagated to all member test threads instead of hanging them.
   */
  @VisibleForTesting
  static void executeBatch(List<PendingItem> items) {
    try {
      executeBatchInternal(items);
    } catch (Throwable t) {
      LOG.error(
          "Unexpected failure while executing merged Dataflow test batch; failing {} tests.",
          items.size(),
          t);
      for (PendingItem item : items) {
        item.resultFuture.completeExceptionally(t);
      }
    } finally {
      for (PendingItem item : items) {
        if (!item.resultFuture.isDone()) {
          item.resultFuture.completeExceptionally(
              new IllegalStateException(
                  "Merged Dataflow test batch finished without a verdict for "
                      + item.options.getAppName()));
        }
      }
    }
  }

  static String scopePrefix(int memberIndex) {
    return "t" + memberIndex;
  }

  private static void executeBatchInternal(List<PendingItem> items) {
    int batchNumber = BATCH_COUNTER.getAndIncrement();
    List<Pipeline> memberPipelines = new ArrayList<>(items.size());
    List<String> scopes = new ArrayList<>(items.size());
    StringBuilder membership = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      PendingItem item = items.get(i);
      item.scopePrefix = scopePrefix(i);
      item.pipeline.setRootNamePrefix(item.scopePrefix);
      memberPipelines.add(item.pipeline);
      scopes.add(item.scopePrefix);
      membership
          .append(i == 0 ? "" : ", ")
          .append(item.scopePrefix)
          .append('=')
          .append(item.options.getAppName());
    }
    LOG.info(
        "Merging {} ValidatesRunner test pipelines into Dataflow batch #{}: {}",
        items.size(),
        batchNumber,
        membership);

    PendingItem leader = items.get(0);
    String originalLeaderJobName = leader.options.getJobName();
    String batchJobName = "batch-" + batchNumber + "-" + originalLeaderJobName;
    if (batchJobName.length() > 63) {
      batchJobName = batchJobName.substring(0, 63);
      while (batchJobName.endsWith("-")) {
        batchJobName = batchJobName.substring(0, batchJobName.length() - 1);
      }
    }
    leader.options.setJobName(batchJobName);
    CompositeBatchPipeline compositePipeline =
        new CompositeBatchPipeline(leader.options, memberPipelines);

    DataflowPipelineJob batchJob = null;
    State terminalState = null;
    try {
      batchJob = leader.delegateRunner.run(compositePipeline);
      LOG.info(
          "Submitted merged Dataflow job {} for batch #{} ({} tests)",
          batchJob.getJobId(),
          batchNumber,
          items.size());
      leader.runner.waitForBatchJobTermination(batchJob);
      terminalState = batchJob.getState();
    } catch (Exception e) {
      LOG.warn(
          "Merged Dataflow batch #{} failed during submission or execution; all {} tests will"
              + " re-run standalone.",
          batchNumber,
          items.size(),
          e);
      cancelQuietly(batchJob);
    } finally {
      leader.options.setJobName(originalLeaderJobName);
    }

    List<BatchOutcome> outcomes = new ArrayList<>(items.size());
    if (batchJob == null || terminalState == null) {
      for (int i = 0; i < items.size(); i++) {
        outcomes.add(BatchOutcome.runStandalone(false));
      }
    } else {
      // Metrics are fetched regardless of terminal state: counters reported by stages that
      // completed before the job failed are retained by the service.
      JobMetrics metrics = leader.runner.getJobMetrics(batchJob);
      StageAttribution attribution = null;
      if (terminalState != State.DONE) {
        attribution = StageAttribution.fromJob(leader.runner.getJobWithExecutionDetails(batchJob));
        if (attribution == null) {
          LOG.warn(
              "Merged Dataflow job {} terminated in state {} and per-stage execution state is"
                  + " unavailable; all {} tests will re-run standalone.",
              batchJob.getJobId(),
              terminalState,
              items.size());
        }
      }
      for (PendingItem item : items) {
        String scope = item.scopePrefix;
        boolean countersOk =
            checkScopedPAssertSuccess(batchJob, metrics, scope, item.expectedAssertions);
        String verdict;
        boolean credited;
        if (terminalState == State.DONE) {
          credited = countersOk;
          verdict = countersOk ? "job DONE, PAsserts verified" : "job DONE but PAsserts unverified";
        } else if (attribution == null) {
          credited = false;
          verdict = "job " + terminalState + ", stage states unavailable";
        } else {
          StageAttribution.ScopeStatus status = attribution.statusForScope(scope, scopes);
          credited = countersOk && status.isComplete();
          verdict =
              "job "
                  + terminalState
                  + ", "
                  + status
                  + (countersOk ? ", PAsserts verified" : ", PAsserts unverified");
        }
        if (credited) {
          LOG.info(
              "Test {} (scope {}) passed in merged Dataflow job {} ({}).",
              item.options.getAppName(),
              scope,
              batchJob.getJobId(),
              verdict);
          outcomes.add(BatchOutcome.passed(new ScopedDataflowPipelineJob(batchJob, scope)));
        } else {
          LOG.warn(
              "Test {} (scope {}) could not be credited from merged Dataflow job {} ({});"
                  + " re-running standalone.",
              item.options.getAppName(),
              scope,
              batchJob.getJobId(),
              verdict);
          outcomes.add(BatchOutcome.runStandalone(false));
        }
      }
    }

    // Complete futures only after every member has been evaluated: a completed member immediately
    // restores (mutates) its own pipeline on its test thread.
    for (int i = 0; i < items.size(); i++) {
      items.get(i).resultFuture.complete(outcomes.get(i));
    }
  }

  private static void cancelQuietly(@Nullable DataflowPipelineJob job) {
    if (job == null) {
      return;
    }
    try {
      if (!job.getState().isTerminal()) {
        LOG.info("Cancelling abandoned merged Dataflow job {}", job.getJobId());
        job.cancel();
      }
    } catch (Exception e) {
      LOG.warn("Failed to cancel merged Dataflow job {}", job.getJobId(), e);
    }
  }

  /**
   * Per-stage execution state of a Dataflow job, attributed to batch member scopes.
   *
   * <p>Attribution is purely by the user-facing names a stage reports (component transforms and
   * sources, input and output sources): a stage "may belong to" a scope if any of its names is
   * under that scope's prefix. No assumption is made about how the service fuses steps into stages:
   * a stage that reports no names, or a name under no known scope, is treated as possibly belonging
   * to every scope. This makes the verdict conservative in exactly the cases where the service's
   * view cannot be mapped back to a member.
   */
  @VisibleForTesting
  static final class StageAttribution {
    private static final String STAGE_DONE = "JOB_STATE_DONE";

    private final Map<String, String> stateByStage;
    private final Map<String, Set<String>> namesByStage;

    private StageAttribution(
        Map<String, String> stateByStage, Map<String, Set<String>> namesByStage) {
      this.stateByStage = stateByStage;
      this.namesByStage = namesByStage;
    }

    /** Returns {@code null} if the job does not carry both stage states and stage descriptions. */
    static @Nullable StageAttribution fromJob(@Nullable Job job) {
      if (job == null
          || job.getStageStates() == null
          || job.getStageStates().isEmpty()
          || job.getPipelineDescription() == null
          || job.getPipelineDescription().getExecutionPipelineStage() == null
          || job.getPipelineDescription().getExecutionPipelineStage().isEmpty()) {
        return null;
      }
      Map<String, String> stateByStage = new HashMap<>();
      for (ExecutionStageState stageState : job.getStageStates()) {
        if (stageState.getExecutionStageName() != null
            && stageState.getExecutionStageState() != null) {
          stateByStage.put(stageState.getExecutionStageName(), stageState.getExecutionStageState());
        }
      }
      Map<String, Set<String>> namesByStage = new LinkedHashMap<>();
      for (ExecutionStageSummary stage : job.getPipelineDescription().getExecutionPipelineStage()) {
        if (stage.getName() == null) {
          continue;
        }
        Set<String> names = new HashSet<>();
        if (stage.getComponentTransform() != null) {
          for (ComponentTransform t : stage.getComponentTransform()) {
            addName(names, t.getUserName());
            addName(names, t.getOriginalTransform());
          }
        }
        if (stage.getComponentSource() != null) {
          for (ComponentSource s : stage.getComponentSource()) {
            addName(names, s.getUserName());
            addName(names, s.getOriginalTransformOrCollection());
          }
        }
        addSourceNames(names, stage.getInputSource());
        addSourceNames(names, stage.getOutputSource());
        namesByStage.put(stage.getName(), names);
      }
      return new StageAttribution(stateByStage, namesByStage);
    }

    private static void addSourceNames(Set<String> names, @Nullable List<StageSource> sources) {
      if (sources == null) {
        return;
      }
      for (StageSource source : sources) {
        addName(names, source.getUserName());
        addName(names, source.getOriginalTransformOrCollection());
      }
    }

    private static void addName(Set<String> names, @Nullable String name) {
      if (name != null && !name.isEmpty()) {
        names.add(name);
      }
    }

    /** Summarises the stages that may belong to one scope. */
    static final class ScopeStatus {
      private final int doneStages;
      private final List<String> incompleteStages;

      private ScopeStatus(int doneStages, List<String> incompleteStages) {
        this.doneStages = doneStages;
        this.incompleteStages = incompleteStages;
      }

      /** True iff at least one stage may belong to the scope and every such stage is DONE. */
      boolean isComplete() {
        return doneStages > 0 && incompleteStages.isEmpty();
      }

      @Override
      public String toString() {
        return doneStages
            + " stage(s) done"
            + (incompleteStages.isEmpty() ? "" : ", not done: " + incompleteStages);
      }
    }

    ScopeStatus statusForScope(String scope, Collection<String> allScopes) {
      int done = 0;
      List<String> incomplete = new ArrayList<>();
      for (Map.Entry<String, Set<String>> stage : namesByStage.entrySet()) {
        if (!mayBelongToScope(stage.getValue(), scope, allScopes)) {
          continue;
        }
        String state = stateByStage.get(stage.getKey());
        if (STAGE_DONE.equals(state)) {
          done++;
        } else {
          incomplete.add(stage.getKey() + "=" + (state == null ? "UNKNOWN" : state));
        }
      }
      return new ScopeStatus(done, incomplete);
    }

    private static boolean mayBelongToScope(
        Set<String> names, String scope, Collection<String> allScopes) {
      if (names.isEmpty()) {
        return true;
      }
      for (String name : names) {
        if (matchesScopePrefix(name, scope)) {
          return true;
        }
        boolean underSomeScope = false;
        for (String other : allScopes) {
          if (matchesScopePrefix(name, other)) {
            underSomeScope = true;
            break;
          }
        }
        if (!underSomeScope) {
          return true;
        }
      }
      return false;
    }
  }

  @VisibleForTesting
  static boolean checkScopedPAssertSuccess(
      DataflowPipelineJob batchJob,
      @Nullable JobMetrics metrics,
      String scopePrefix,
      int expectedAssertions) {
    if (metrics == null || metrics.getMetrics() == null) {
      return false;
    }
    int successes = 0;
    int failures = 0;
    for (MetricUpdate metric : metrics.getMetrics()) {
      if (metric.getName() == null
          || metric.getName().getContext() == null
          || !metric.getName().getContext().containsKey(TENTATIVE_COUNTER)) {
        continue;
      }
      String internalStepName = metric.getName().getContext().get("step");
      if (internalStepName == null) {
        continue;
      }
      String userStepName = resolveUserStepName(batchJob, internalStepName);
      if (userStepName == null || !matchesScopePrefix(userStepName, scopePrefix)) {
        continue;
      }
      if (PAssert.SUCCESS_COUNTER.equals(metric.getName().getName())) {
        successes += ((BigDecimal) metric.getScalar()).intValue();
      } else if (PAssert.FAILURE_COUNTER.equals(metric.getName().getName())) {
        failures += ((BigDecimal) metric.getScalar()).intValue();
      }
    }
    return failures == 0 && successes >= expectedAssertions;
  }

  static @Nullable String resolveUserStepName(DataflowPipelineJob job, String internalStepName) {
    RunnerApi.@Nullable Pipeline pipelineProto = job.getPipelineProto();
    if (pipelineProto != null) {
      RunnerApi.@Nullable PTransform transform =
          pipelineProto.getComponents().getTransformsMap().get(internalStepName);
      if (transform != null && !transform.getUniqueName().isEmpty()) {
        return transform.getUniqueName();
      }
    }
    BiMap<AppliedPTransform<?, ?, ?>, String> stepNames = job.getTransformStepNames();
    if (stepNames != null) {
      AppliedPTransform<?, ?, ?> applied = stepNames.inverse().get(internalStepName);
      if (applied != null) {
        return applied.getFullName();
      }
    }
    return internalStepName;
  }

  static boolean matchesScopePrefix(String stepName, String scopePrefix) {
    return stepName.startsWith(scopePrefix + "/")
        || stepName.startsWith(scopePrefix + "-")
        || stepName.equals(scopePrefix);
  }

  static String stripScopePrefix(String stepName, String scopePrefix) {
    if (stepName.startsWith(scopePrefix + "/") || stepName.startsWith(scopePrefix + "-")) {
      return stepName.substring(scopePrefix.length() + 1);
    }
    if (stepName.equals(scopePrefix)) {
      return "";
    }
    return stepName;
  }

  /**
   * A {@link DataflowPipelineJob} view for a single member pipeline within a merged batch job that
   * filters and unprefixes {@link MetricResults} to that member's transform scope.
   *
   * <p>Instances are only ever handed to members that were <em>credited</em> by the coordinator,
   * i.e. every execution stage belonging to the member finished successfully and its PAssert
   * counters were satisfied. The underlying batch job may nevertheless be in a non-{@code DONE}
   * terminal state because of a <em>different</em> member, so this view always reports {@link
   * State#DONE} rather than delegating to the batch job's state.
   */
  static final class ScopedDataflowPipelineJob extends DataflowPipelineJob {
    private final MetricResults scopedMetrics;

    ScopedDataflowPipelineJob(DataflowPipelineJob delegate, String scopePrefix) {
      super(
          null,
          delegate.getJobId(),
          delegate.getDataflowOptions(),
          delegate.getTransformStepNames(),
          delegate.getPipelineProto());
      this.scopedMetrics = new ScopedMetricResults(delegate, scopePrefix);
    }

    @Override
    public State getState() {
      return State.DONE;
    }

    @Override
    public @Nullable State waitUntilFinish() {
      return State.DONE;
    }

    @Override
    public @Nullable State waitUntilFinish(Duration duration) {
      return State.DONE;
    }

    @Override
    public @Nullable State waitUntilFinish(
        Duration duration, MonitoringUtil.JobMessagesHandler messageHandler) {
      return State.DONE;
    }

    @Override
    public State cancel() {
      // The member's work is already complete; cancelling is a no-op and must not affect the
      // other members sharing the batch job.
      return State.DONE;
    }

    @Override
    public MetricResults metrics() {
      return scopedMetrics;
    }
  }

  private static final class ScopedMetricResults extends MetricResults {
    private final DataflowPipelineJob delegateJob;
    private final String scopePrefix;

    private ScopedMetricResults(DataflowPipelineJob delegateJob, String scopePrefix) {
      this.delegateJob = delegateJob;
      this.scopePrefix = scopePrefix;
    }

    @Override
    public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
      MetricsFilter.Builder nameOnlyFilterBuilder = MetricsFilter.builder();
      if (filter != null) {
        for (MetricNameFilter nameFilter : filter.names()) {
          nameOnlyFilterBuilder.addNameFilter(nameFilter);
        }
      }
      MetricQueryResults rawResults;
      synchronized (delegateJob) {
        rawResults = delegateJob.metrics().queryMetrics(nameOnlyFilterBuilder.build());
      }
      return MetricQueryResults.create(
          filterAndStrip(rawResults.getCounters(), filter),
          filterAndStrip(rawResults.getDistributions(), filter),
          filterAndStrip(rawResults.getGauges(), filter),
          filterAndStrip(rawResults.getStringSets(), filter),
          filterAndStrip(rawResults.getBoundedTries(), filter),
          Collections.<MetricResult<HistogramData>>emptyList());
    }

    private <T> List<MetricResult<T>> filterAndStrip(
        Iterable<MetricResult<T>> results, @Nullable MetricsFilter filter) {
      List<MetricResult<T>> scoped = new ArrayList<>();
      for (MetricResult<T> result : results) {
        MetricKey key = result.getKey();
        String stepName = key.stepName();
        if (stepName == null || !matchesScopePrefix(stepName, scopePrefix)) {
          continue;
        }
        String unprefixedStep = stripScopePrefix(stepName, scopePrefix);
        MetricKey unprefixedKey = MetricKey.create(unprefixedStep, key.metricName());
        if (filter == null || MetricFiltering.matches(filter, unprefixedKey)) {
          scoped.add(
              MetricResult.create(
                  unprefixedKey, result.getCommittedOrNull(), result.getAttempted()));
        }
      }
      return scoped;
    }
  }
}
