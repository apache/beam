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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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
import com.google.api.services.dataflow.model.StageSource;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.TestDataflowRunner.ErrorMonitorMessagesHandler;
import org.apache.beam.runners.dataflow.TestDataflowRunner.PAssertCounts;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.CompositePipeline;
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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.BeamParallelJunit4Runner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatchers;
import org.apache.beam.sdk.testing.TestPipeline;
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
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates merging concurrent batch {@code TestPipeline} executions within a JVM into a single
 * Dataflow job per batch, with scoped {@link PAssert} and {@link MetricResults} verification.
 *
 * <p>Batching spans runner instances: every test builds its own {@link TestDataflowRunner}, and the
 * pipelines merged into one job come from different tests, threads and classes. Runners therefore
 * share the single process-wide coordinator returned by {@link #shared()}; tests may construct
 * private instances instead.
 *
 * <p>Scoping: members of a merged job are told apart by their {@linkplain TestPipeline#getRootName
 * unique root name}, which {@link TestPipeline} assigns (when {@code
 * -DbeamTestPipelineUniqueRootNames=true}) before the test applies any transform, so that every
 * step, PCollection and metric of a member is named {@code <root>/...}. The coordinator never
 * renames or otherwise mutates a member pipeline; a pipeline without a root name simply runs as its
 * own job.
 *
 * <p>Threading model: a test thread only enqueues its pipeline and then blocks on a per-pipeline
 * future. Per {@linkplain #compatibilityKey options-compatibility group}, a daemon scheduler thread
 * drains the queue and groups pipelines into batches; each closed batch is handed to a daemon
 * executor that submits the merged job, waits for it and reaches a verdict for every member. A
 * member is credited from the merged job when its scoped PAsserts are verified and &mdash; if the
 * job did not end {@code DONE} &mdash; every stage that may belong to it is {@code DONE} (see
 * {@link StageAttribution}). Any other member gets {@link TestPipeline.StandaloneRerunRequested}
 * out of its {@code run()}, and its test is executed again from scratch as a job of its own by
 * {@link BeamParallelJunit4Runner}: the member's pipeline object is never run a second time, since
 * building the merged job rewrote it with the leader's runner. (A pipeline for which no merged job
 * was built at all, because no partner arrived in time, simply runs standalone on its own test
 * thread.) No test thread ever performs work on behalf of another test, so a test timeout or
 * interrupt affects only that test.
 *
 * <p>No-hang guarantee: a member's future is always completed, whatever happens on the scheduler or
 * batch-runner threads (see {@link BatchCollector} and {@link #executeBatch}); the merged job is
 * waited on for at most the shortest member test timeout ({@link #mergedJobTimeout}); and as a last
 * resort each test thread's wait is bounded ({@link PendingItem#awaitResult()}), so an unforeseen
 * failure surfaces as a diagnosable test failure rather than a fork that never finishes.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class DataflowTestBatchCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowTestBatchCoordinator.class);

  static final String TEST_BATCHING_PROPERTY = "beam.dataflow.testBatching";
  static final String TEST_BATCH_MAX_SIZE_PROPERTY = "beam.dataflow.testBatchMaxSize";
  static final String TEST_BATCH_WINDOW_MS_PROPERTY = "beam.dataflow.testBatchWindowMs";

  private static final long DEFAULT_BATCH_WINDOW_MS = 3000L;
  private static final int DEFAULT_BATCH_MAX_SIZE = 30;

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
      // Unless sized explicitly, a batch may hold one pipeline per concurrently running test.
      int defaultSize =
          Integer.getInteger(
              BeamParallelJunit4Runner.VALIDATES_RUNNER_THREADS_PROPERTY, DEFAULT_BATCH_MAX_SIZE);
      return new BatchingConfig(
          Boolean.getBoolean(TEST_BATCHING_PROPERTY),
          Integer.getInteger(TEST_BATCH_MAX_SIZE_PROPERTY, Math.max(1, defaultSize)),
          Long.getLong(TEST_BATCH_WINDOW_MS_PROPERTY, DEFAULT_BATCH_WINDOW_MS));
    }
  }

  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(1);
  private static volatile @Nullable DataflowTestBatchCoordinator shared;

  /** The process-wide coordinator shared by all {@link TestDataflowRunner} instances. */
  static DataflowTestBatchCoordinator shared() {
    DataflowTestBatchCoordinator result = shared;
    if (result == null) {
      synchronized (DataflowTestBatchCoordinator.class) {
        result = shared;
        if (result == null) {
          result = new DataflowTestBatchCoordinator(BatchingConfig.fromSystemProperties());
          shared = result;
        }
      }
    }
    return result;
  }

  private final BatchingConfig config;
  private final String name;
  private final AtomicInteger batchCounter = new AtomicInteger(1);
  private final AtomicInteger collectorCounter = new AtomicInteger(1);
  private final ConcurrentHashMap<String, BatchCollector> collectors = new ConcurrentHashMap<>();
  private volatile boolean closed;

  /** Runs merged jobs so that schedulers keep collecting while jobs are in flight. */
  private final ExecutorService batchRunners;

  DataflowTestBatchCoordinator(BatchingConfig config) {
    this.config = config;
    this.name = "beam-test-batch-" + INSTANCE_COUNTER.getAndIncrement();
    this.batchRunners = Executors.newCachedThreadPool(daemonThreadFactory(name + "-runner-"));
  }

  /**
   * Stops the scheduler threads and the batch-runner pool. Members still waiting for a verdict fail
   * promptly and later submissions are rejected. Tests only.
   */
  @VisibleForTesting
  void shutdown() {
    closed = true;
    for (BatchCollector collector : collectors.values()) {
      collector.stop();
    }
    batchRunners.shutdownNow();
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

  /**
   * Returns a key identifying the group of pipelines that may share a merged job with one built
   * from {@code options}, or {@code null} if the options cannot be serialized, in which case the
   * pipeline must run standalone.
   *
   * <p>The key is a digest of every option that has been explicitly set on the options object
   * through <em>any</em> {@link PipelineOptions} interface it has been viewed as, minus {@link
   * #PER_TEST_OPTIONS}. Deriving it from the serialized form rather than from a hand-picked list of
   * getters means that an option added to any interface (worker pool, debug, GCP, SDK harness,
   * user-defined, ...) automatically splits batches when it differs instead of being silently
   * dropped for all but the first member.
   *
   * <p>Defaults that were merely bound by a getter are left out. The serialized form contains them
   * too, but which defaults are bound depends on which getters the test's transforms happened to
   * call during construction (for example {@code View.asList()} and {@code Reshuffle} read {@code
   * updateCompatibilityVersion}), and two tests must not end up in different batches because one of
   * them read an option both would have received the same default for. {@link
   * org.apache.beam.sdk.testing.TestPipeline} hands the runner a JSON copy of the test's options;
   * it serializes that copy with {@link PipelineOptionsFactory#SERIALIZE_DEFAULTS_ATTRIBUTE} so
   * that the distinction survives the round trip.
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
    Set<String> explicitlySet;
    try {
      serialized = MAPPER.valueToTree(options).get("options");
      explicitlySet = PipelineOptionsFactory.explicitlySetProperties(options);
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
      if (explicitlySet.contains(entry.getKey()) && !PER_TEST_OPTIONS.contains(entry.getKey())) {
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

  boolean isEligibleForBatching(Pipeline pipeline, TestDataflowPipelineOptions options) {
    if (!config.enabled || config.maxBatchSize <= 1) {
      return false;
    }
    if (!options.isBlockOnRun()) {
      return false;
    }
    if (options.isStreaming()) {
      // Streaming batches need their own verdict strategy (see executeBatchInternal); until that
      // exists streaming tests always run as individual jobs. Say so once so the flag is not
      // silently ignored by the streaming ValidatesRunner tasks.
      if (STREAMING_UNSUPPORTED_LOGGED.compareAndSet(false, true)) {
        LOG.info(
            "Dataflow test batching does not yet support streaming jobs; streaming tests run as"
                + " individual jobs.");
      }
      return false;
    }
    if (!(pipeline instanceof TestPipeline)) {
      return false;
    }
    TestPipeline testPipeline = (TestPipeline) pipeline;
    if (testPipeline.isStandaloneExecutionRequired()) {
      return false;
    }
    if (testPipeline.getRootName().isEmpty()) {
      // Members of a merged job are told apart purely by their root name, so an unnamed pipeline
      // can never be merged. This is a configuration problem worth pointing out, but only once.
      if (UNNAMED_PIPELINE_WARNED.compareAndSet(false, true)) {
        LOG.warn(
            "Dataflow test batching is enabled but test pipelines have no unique root name, so"
                + " every test runs as its own job. Set -D{}=true to enable merging.",
            TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES);
      }
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

  private static final AtomicBoolean UNNAMED_PIPELINE_WARNED = new AtomicBoolean(false);
  private static final AtomicBoolean STREAMING_UNSUPPORTED_LOGGED = new AtomicBoolean(false);

  private static boolean isDefaultMatcher(@Nullable Object matcher) {
    return matcher == null
        || matcher instanceof TestPipelineOptions.AlwaysPassMatcher
        || SerializableMatchers.anything().equals(matcher);
  }

  /**
   * Submits {@code pipeline} for batched execution and blocks until a verdict has been reached.
   * Callers must have checked {@link #isEligibleForBatching} first; in particular the pipeline must
   * have a unique root name.
   */
  DataflowPipelineJob runInBatch(
      TestPipeline pipeline,
      TestDataflowPipelineOptions options,
      TestDataflowRunner runner,
      DataflowRunner delegateRunner) {
    final String canonicalOptions = canonicalOptionsJson(options);
    if (canonicalOptions == null) {
      return runner.runStandalone(pipeline, delegateRunner);
    }
    String key = digest(canonicalOptions);
    PendingItem item =
        new PendingItem(pipeline, options, runner, delegateRunner, verdictTimeoutMillis(options));
    while (true) {
      if (closed) {
        throw new IllegalStateException(
            "Dataflow test batch coordinator " + name + " has been shut down");
      }
      BatchCollector collector =
          collectors.computeIfAbsent(
              key,
              k -> {
                BatchCollector created =
                    new BatchCollector(k, name + "-group-" + collectorCounter.getAndIncrement());
                LOG.info(
                    "Created Dataflow test batch group {} ({}) for options compatible with {}",
                    created.name,
                    k,
                    options.getAppName());
                LOG.debug("Batch group {} options: {}", created.name, canonicalOptions);
                created.start();
                return created;
              });
      if (collector.enqueue(item)) {
        break;
      }
      // The collector's scheduler stopped between lookup and enqueue; make sure it is unregistered
      // and try again with a fresh one.
      collectors.remove(key, collector);
    }
    return item.awaitResult();
  }

  /**
   * How long a test thread waits for the batch machinery's verdict before failing its test, or a
   * negative value for no limit. Every path through the machinery completes the member's future, so
   * this is a safety net that turns an unknown bug into a diagnosable failure rather than a hung
   * fork. It is deliberately generous: the merged job itself is only waited on for the shortest
   * member test timeout (see {@link #mergedJobTimeout}); the rest is slack for queueing (at most
   * two batch windows), submission, metrics retrieval and cancellation.
   */
  private long verdictTimeoutMillis(TestDataflowPipelineOptions options) {
    Long testTimeoutSeconds = options.getTestTimeoutSeconds();
    if (testTimeoutSeconds == null || testTimeoutSeconds <= 0) {
      return -1;
    }
    return 2 * config.windowMs + 2 * TimeUnit.SECONDS.toMillis(testTimeoutSeconds);
  }

  /**
   * The bound on waiting for a merged job: the shortest {@code testTimeoutSeconds} among its
   * members, so that no member waits longer for the merged job than it would have for its own
   * standalone job. Negative (unbounded) only if no member has a timeout configured.
   */
  @VisibleForTesting
  static Duration mergedJobTimeout(List<PendingItem> items) {
    long minSeconds = Long.MAX_VALUE;
    for (PendingItem item : items) {
      Long timeout = item.options.getTestTimeoutSeconds();
      if (timeout != null && timeout > 0) {
        minSeconds = Math.min(minSeconds, timeout);
      }
    }
    return Duration.standardSeconds(minSeconds == Long.MAX_VALUE ? -1 : minSeconds);
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
   * The verdict the batch machinery reaches for one member pipeline: credited with a {@link #job}
   * from the merged run, declined before any merged job was built from it, or in need of a fresh
   * standalone execution after a merged attempt that yielded no verdict for it.
   */
  @VisibleForTesting
  static final class BatchOutcome {
    enum Kind {
      /** Credited from the merged job; {@link #job} is the member's scoped view of it. */
      PASSED,
      /**
       * Batching was declined before any merged job was built from the pipeline (for example, no
       * partner arrived within the window). The pipeline is untouched, so the member's own test
       * thread runs it standalone exactly as if it had been ineligible.
       */
      DECLINED,
      /**
       * A merged job was built from the pipeline but did not yield a verdict for this member. The
       * pipeline object is not run again (building the merged job rewrote it with another runner);
       * the member's test is re-executed from scratch instead, see {@link
       * TestPipeline.StandaloneRerunRequested}.
       */
      RERUN_REQUIRED
    }

    final Kind kind;
    final @Nullable DataflowPipelineJob job;

    /** Why no verdict could be reached; {@link Kind#RERUN_REQUIRED} only. */
    final @Nullable String reason;

    private BatchOutcome(Kind kind, @Nullable DataflowPipelineJob job, @Nullable String reason) {
      this.kind = kind;
      this.job = job;
      this.reason = reason;
    }

    static BatchOutcome passed(DataflowPipelineJob job) {
      return new BatchOutcome(Kind.PASSED, job, null);
    }

    static BatchOutcome declined() {
      return new BatchOutcome(Kind.DECLINED, null, null);
    }

    static BatchOutcome rerunRequired(String reason) {
      return new BatchOutcome(Kind.RERUN_REQUIRED, null, reason);
    }
  }

  @VisibleForTesting
  static final class PendingItem {
    final TestPipeline pipeline;
    final TestDataflowPipelineOptions options;
    final TestDataflowRunner runner;
    final DataflowRunner delegateRunner;
    final int expectedAssertions;
    final long verdictTimeoutMillis;
    final CompletableFuture<BatchOutcome> resultFuture = new CompletableFuture<>();

    /**
     * The pipeline's unique root name (see {@link TestPipeline#getRootName()}). Every transform and
     * PCollection of this member is named {@code <scope>/...}, which is how the member's steps,
     * stages and metrics are told apart from the other members' in the merged job.
     */
    final String scope;

    /** Assigned by the batch runner once the merged job is submitted; {@code null} before that. */
    volatile @Nullable String mergedJobId;

    PendingItem(
        TestPipeline pipeline,
        TestDataflowPipelineOptions options,
        TestDataflowRunner runner,
        DataflowRunner delegateRunner,
        long verdictTimeoutMillis) {
      this.pipeline = pipeline;
      this.options = options;
      this.runner = runner;
      this.delegateRunner = delegateRunner;
      this.verdictTimeoutMillis = verdictTimeoutMillis;
      this.expectedAssertions = PAssert.countAsserts(pipeline);
      this.scope = pipeline.getRootName();
      checkArgument(!scope.isEmpty(), "Batched test pipelines must have a root name");
    }

    /**
     * Blocks the calling (test) thread until the batch machinery has reached a verdict, then
     * returns the merged job, runs this pipeline standalone on the calling thread (if batching was
     * declined before a merged job was built from it), or throws {@link
     * TestPipeline.StandaloneRerunRequested} so that the test runner executes the test again from
     * scratch as a job of its own.
     *
     * <p>The wait is bounded by {@link #verdictTimeoutMillis}. Exceeding it means the machinery
     * failed to deliver a verdict at all, which is a harness bug; the test fails with a description
     * of where the member got stuck rather than silently hanging its fork.
     */
    DataflowPipelineJob awaitResult() {
      BatchOutcome outcome;
      try {
        outcome =
            verdictTimeoutMillis < 0
                ? resultFuture.get()
                : resultFuture.get(verdictTimeoutMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while waiting for the merged Dataflow test batch containing "
                + options.getAppName(),
            e);
      } catch (TimeoutException e) {
        String jobId = mergedJobId;
        throw new IllegalStateException(
            String.format(
                "No verdict for test %s from the Dataflow test batch machinery after %d ms"
                    + " (batch scope: %s; merged job: %s). This is a bug in the test batching"
                    + " machinery, not in the test.",
                options.getAppName(),
                verdictTimeoutMillis,
                scope,
                jobId == null ? "never submitted" : jobId),
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
      switch (outcome.kind) {
        case PASSED:
          return outcome.job;
        case DECLINED:
          return runner.runStandalone(pipeline, delegateRunner);
        case RERUN_REQUIRED:
        default:
          String jobId = mergedJobId;
          throw new TestPipeline.StandaloneRerunRequested(
              String.format(
                  "Test %s (scope %s) was merged into Dataflow job %s, which yielded no verdict"
                      + " for it: %s. The test has to be executed again as a job of its own;"
                      + " BeamParallelJunit4Runner does so automatically.",
                  options.getAppName(),
                  scope,
                  jobId == null ? "(never submitted)" : jobId,
                  outcome.reason));
      }
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
   * Groups pipelines that share an options-compatibility key into batches.
   *
   * <p>Test threads only {@link #enqueue} and then wait on their {@link PendingItem#resultFuture}.
   * A single daemon scheduler thread drains the queue: it takes the first pipeline, waits up to the
   * batch window for more (closing early once the batch is full), and hands the closed batch to the
   * coordinator's {@link #batchRunners} so that it can immediately start collecting the next batch
   * while the merged job is in flight. A batch of one is not worth a merged job and is sent
   * straight back to its test thread to run standalone.
   *
   * <p>Lifecycle: however the scheduler thread ends (interrupt, or an unexpected error), the
   * collector {@link #close}s: it marks itself closed, unregisters from the coordinator so that the
   * next submission gets a fresh collector, and fails everything still queued. {@link #enqueue}
   * cooperates with that so no item can be added after the final drain and never be seen again.
   */
  private final class BatchCollector {
    private final String key;
    private final String name;
    private final LinkedBlockingQueue<PendingItem> queue = new LinkedBlockingQueue<>();
    private @Nullable Thread scheduler;

    /** Once set, nothing in {@link #queue} will ever be dispatched; see {@link #enqueue}. */
    private volatile boolean closed;

    BatchCollector(String key, String name) {
      this.key = key;
      this.name = name;
    }

    synchronized void start() {
      if (scheduler == null) {
        scheduler = new Thread(this::schedulerLoop, name + "-scheduler");
        scheduler.setDaemon(true);
        scheduler.start();
      }
    }

    synchronized void stop() {
      if (scheduler != null) {
        scheduler.interrupt();
      }
    }

    /**
     * Returns {@code false} if this collector is already closed and the caller must use another
     * one. Returns {@code true} once {@code item} is guaranteed a verdict: either the scheduler
     * dispatches it, or it is failed when the collector closes.
     */
    boolean enqueue(PendingItem item) {
      if (closed) {
        return false;
      }
      queue.add(item);
      if (closed) {
        // Raced with close(): its drain may have run before our add. Draining again is idempotent
        // (futures complete at most once), so whichever side sees the item fails it.
        failQueued(null);
      }
      return true;
    }

    private void schedulerLoop() {
      Throwable failure = null;
      try {
        while (true) {
          dispatch(collectBatch(queue.take()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        failure = t;
        LOG.error(
            "Dataflow test batch scheduler {} died unexpectedly; queued tests will fail and later"
                + " tests will use a new scheduler.",
            name,
            t);
      } finally {
        close(failure);
      }
    }

    private void close(@Nullable Throwable cause) {
      closed = true;
      collectors.remove(key, this);
      failQueued(cause);
    }

    private void failQueued(@Nullable Throwable cause) {
      List<PendingItem> orphans = new ArrayList<>();
      queue.drainTo(orphans);
      for (PendingItem orphan : orphans) {
        orphan.resultFuture.completeExceptionally(
            new IllegalStateException(
                "Dataflow test batch scheduler "
                    + name
                    + " stopped before scheduling "
                    + orphan.options.getAppName(),
                cause));
      }
    }

    private List<PendingItem> collectBatch(PendingItem first) throws InterruptedException {
      List<PendingItem> batch = new ArrayList<>();
      batch.add(first);
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.windowMs);
      try {
        while (batch.size() < config.maxBatchSize) {
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
      } catch (InterruptedException e) {
        // Hand the partial batch back so that close() fails these members too.
        queue.addAll(batch);
        throw e;
      }
      return batch;
    }

    /**
     * Hands a closed batch off without doing any job work on the scheduler thread; this returns
     * immediately either way.
     *
     * <p>A batch of one is not executed through {@link #executeBatch}: that would wrap the lone
     * pipeline in a renamed composite, apply scoped PAssert/stage verification, and &mdash; on
     * failure &mdash; require the test to be executed again. Instead the member is told that
     * batching was declined, which makes it behave exactly like an ineligible pipeline: its own
     * test thread runs it standalone (see {@link PendingItem#awaitResult()}) with the full {@link
     * TestDataflowRunner} semantics and unmodified names.
     */
    private void dispatch(List<PendingItem> batch) {
      try {
        if (batch.size() == 1) {
          // Verdict only; the job is submitted by the test thread.
          batch.get(0).resultFuture.complete(BatchOutcome.declined());
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
  void executeBatch(List<PendingItem> items) {
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

  /**
   * The most conservative Dataflow job-name length we know of. The current REST reference allows
   * {@code [a-z]([-a-z0-9]{0,1022}[a-z0-9])?}, but the service historically enforced 63 (and the
   * Python SDK still tests against that), so merged job names are kept within it.
   */
  @VisibleForTesting static final int MAX_JOB_NAME_LENGTH = 63;

  /**
   * Every merged job's name starts with this, which is how a pipeline running on Dataflow can tell
   * (via its {@code jobName} option) that it is part of a merged job rather than a standalone one.
   */
  static final String BATCH_JOB_NAME_PREFIX = "batch-";

  private static final DateTimeFormatter JOB_NAME_TIMESTAMP =
      DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

  /** Builds the merged job's name; see {@link #batchJobName(int, String, long, int)}. */
  private static String batchJobName(int batchNumber, @Nullable String leaderAppName) {
    return batchJobName(
        batchNumber,
        leaderAppName,
        System.currentTimeMillis(),
        ThreadLocalRandom.current().nextInt());
  }

  /**
   * Builds {@code batch-<n>-<app hint>-<MMddHHmmss>-<random hex>} within {@link
   * #MAX_JOB_NAME_LENGTH}.
   *
   * <p>Uniqueness comes from the timestamp and random suffix, the same scheme as {@code
   * PipelineOptions.JobNameFactory} uses for standalone jobs. It must not come from the batch
   * number: that counter is per JVM, and the same test classes run concurrently in several JVMs and
   * postcommits that all count from one. A colliding name makes Dataflow return the other job and
   * the whole batch falls back to standalone runs. The leader's app name is only a hint for humans
   * reading the console and is the only part that is ever truncated.
   */
  @VisibleForTesting
  static String batchJobName(
      int batchNumber, @Nullable String leaderAppName, long nowMillis, int random) {
    String prefix = BATCH_JOB_NAME_PREFIX + batchNumber;
    String suffix = "-" + JOB_NAME_TIMESTAMP.print(nowMillis) + "-" + Integer.toHexString(random);
    String hint =
        leaderAppName == null ? "" : leaderAppName.toLowerCase().replaceAll("[^a-z0-9]", "0");
    int room = MAX_JOB_NAME_LENGTH - prefix.length() - suffix.length() - 1;
    if (hint.length() > room) {
      hint = hint.substring(0, Math.max(0, room));
    }
    return hint.isEmpty() ? prefix + suffix : prefix + "-" + hint + suffix;
  }

  private void executeBatchInternal(List<PendingItem> items) {
    int batchNumber = batchCounter.getAndIncrement();
    List<Pipeline> memberPipelines = new ArrayList<>(items.size());
    List<String> scopes = new ArrayList<>(items.size());
    StringBuilder membership = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      PendingItem item = items.get(i);
      memberPipelines.add(item.pipeline);
      scopes.add(item.scope);
      membership
          .append(i == 0 ? "" : ", ")
          .append(item.scope)
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
    String batchJobName = batchJobName(batchNumber, leader.options.getAppName());
    leader.options.setJobName(batchJobName);

    DataflowPipelineJob batchJob = null;
    State terminalState = null;
    ErrorMonitorMessagesHandler messages = null;
    String noVerdictReason = null;
    try {
      // The constructor rejects members whose transform names collide. That cannot happen with
      // TestPipeline's sequence-numbered root names, but if it did the catch below sends every
      // member back for a fresh standalone execution rather than risk mis-crediting a test.
      CompositePipeline compositePipeline = new CompositePipeline(leader.options, memberPipelines);
      batchJob = leader.runner.submit(leader.delegateRunner, compositePipeline);
      for (PendingItem item : items) {
        item.mergedJobId = batchJob.getJobId();
      }
      LOG.info(
          "Submitted merged Dataflow job {} for batch #{} ({} tests)",
          batchJob.getJobId(),
          batchNumber,
          items.size());
      messages = TestDataflowRunner.errorMonitorFor(batchJob);
      Duration timeout = mergedJobTimeout(items);
      terminalState = leader.runner.waitForMergedJobTermination(batchJob, timeout, messages);
      if (terminalState == null) {
        noVerdictReason =
            "the merged job did not terminate within " + timeout + " and was cancelled";
        LOG.warn(
            "Merged Dataflow job {} for batch #{} did not terminate within {}; cancelling it; all"
                + " {} tests will be executed again standalone.{}",
            batchJob.getJobId(),
            batchNumber,
            timeout,
            items.size(),
            errorSuffix(messages));
        cancelQuietly(batchJob);
      }
    } catch (Exception e) {
      noVerdictReason = "the merged job failed during submission or execution (" + e + ")";
      LOG.warn(
          "Merged Dataflow batch #{} failed during submission or execution; all {} tests will be"
              + " executed again standalone.{}",
          batchNumber,
          items.size(),
          errorSuffix(messages),
          e);
      cancelQuietly(batchJob);
    } finally {
      leader.options.setJobName(originalLeaderJobName);
    }

    List<BatchOutcome> outcomes;
    if (batchJob == null || terminalState == null) {
      outcomes = new ArrayList<>(items.size());
      for (int i = 0; i < items.size(); i++) {
        outcomes.add(BatchOutcome.rerunRequired(noVerdictReason + errorSuffix(messages)));
      }
    } else {
      // How members are credited depends on the execution mode, and all members of a batch share
      // one mode because `streaming` is part of the compatibility key. Only batch mode is eligible
      // today (see isEligibleForBatching); a streaming strategy would plug in here, e.g. crediting
      // everyone on DONE without error messages and nobody otherwise, since streaming jobs expose
      // neither per-stage terminal states nor the batch verification counters.
      checkState(!leader.options.isStreaming(), "Streaming batches are not supported yet");
      outcomes = batchModeVerdicts(batchJob, terminalState, messages, items, scopes);
    }

    // Complete futures only after every member has been evaluated.
    for (int i = 0; i < items.size(); i++) {
      items.get(i).resultFuture.complete(outcomes.get(i));
    }
  }

  /**
   * Credits members of a terminated <i>batch-mode</i> merged job. A member is credited when its
   * scoped PAssert counters verify and, unless the job ended {@code DONE}, every stage that may
   * belong to it ended {@code DONE} (see {@link StageAttribution}); anyone else must be executed
   * again standalone.
   */
  private List<BatchOutcome> batchModeVerdicts(
      DataflowPipelineJob batchJob,
      State terminalState,
      ErrorMonitorMessagesHandler messages,
      List<PendingItem> items,
      List<String> scopes) {
    List<BatchOutcome> outcomes = new ArrayList<>(items.size());
    // Metrics are fetched regardless of terminal state: counters reported by stages that
    // completed before the job failed are retained by the service.
    JobMetrics metrics = leader(items).runner.getJobMetrics(batchJob);
    StageAttribution attribution = null;
    if (terminalState != State.DONE) {
      attribution =
          StageAttribution.fromJob(leader(items).runner.getJobWithExecutionDetails(batchJob));
      if (attribution == null) {
        LOG.warn(
            "Merged Dataflow job {} terminated in state {} and per-stage execution state is"
                + " unavailable; all {} tests will be executed again standalone.{}",
            batchJob.getJobId(),
            terminalState,
            items.size(),
            errorSuffix(messages));
      }
    }
    for (PendingItem item : items) {
      String scope = item.scope;
      PAssertCounts counts = scopedPAssertCounts(batchJob, metrics, scope);
      boolean countersOk = counts != null && counts.satisfy(item.expectedAssertions);
      String passertVerdict =
          (countersOk ? "PAsserts verified (" : "PAsserts unverified (")
              + (counts == null ? "metrics unavailable" : counts)
              + " out of "
              + item.expectedAssertions
              + " expected assertions)";
      String verdict;
      boolean credited;
      if (terminalState == State.DONE) {
        credited = countersOk;
        verdict = "job DONE, " + passertVerdict;
      } else if (attribution == null) {
        credited = false;
        verdict = "job " + terminalState + ", stage states unavailable";
      } else {
        StageAttribution.ScopeStatus status = attribution.statusForScope(scope, scopes);
        credited = countersOk && status.isComplete();
        verdict = "job " + terminalState + ", " + status + ", " + passertVerdict;
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
            "Test {} (scope {}) could not be credited from merged Dataflow job {} ({}); it will be"
                + " executed again standalone.{}",
            item.options.getAppName(),
            scope,
            batchJob.getJobId(),
            verdict,
            errorSuffix(messages));
        outcomes.add(BatchOutcome.rerunRequired(verdict + errorSuffix(messages)));
      }
    }
    return outcomes;
  }

  private static PendingItem leader(List<PendingItem> items) {
    return items.get(0);
  }

  /** The job's error messages, formatted for appending to a log line, or empty if none. */
  private static String errorSuffix(@Nullable ErrorMonitorMessagesHandler messages) {
    if (messages == null || !messages.hasSeenError()) {
      return "";
    }
    return " Job errors: " + messages.getErrorMessage();
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

  /**
   * Sums the {@link PAssert} counters of the steps under {@code scopePrefix} in a merged job, or
   * returns {@code null} if the job's metrics are unavailable.
   */
  static @Nullable PAssertCounts scopedPAssertCounts(
      DataflowPipelineJob batchJob, @Nullable JobMetrics metrics, String scopePrefix) {
    if (metrics == null || metrics.getMetrics() == null) {
      return null;
    }
    return TestDataflowRunner.countPAssertCounters(
        metrics,
        internalStepName -> {
          if (internalStepName.isEmpty()) {
            return false;
          }
          String userStepName = resolveUserStepName(batchJob, internalStepName);
          return userStepName != null && matchesScopePrefix(userStepName, scopePrefix);
        });
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

  /**
   * Whether a step, stage or metric name belongs to the member whose root is {@code scopePrefix}.
   *
   * <p>Beam full names are {@code /}-separated ({@code t3-Foo-bar/ParDo(Anon)}), but Runner V2
   * stage descriptions and some step names come back mangled with {@code -} in place of {@code /}
   * and output suffixes appended ({@code t3-Foo-bar-ParDo-Anon--out0}), so both separators are
   * accepted. A separator (or an exact match) is required so that {@code t1} does not claim {@code
   * t10/...}.
   */
  static boolean matchesScopePrefix(String stepName, String scopePrefix) {
    return stepName.startsWith(scopePrefix + "/")
        || stepName.startsWith(scopePrefix + "-")
        || stepName.equals(scopePrefix);
  }

  /** Inverse of {@link #matchesScopePrefix}: removes the root and its separator, if present. */
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
   *
   * <p>The view deliberately holds no {@link DataflowClient}: it must never act on the shared batch
   * job on behalf of one member (cancelling or draining it would abort the other members' work),
   * and its state is already known. Every public operation that would otherwise contact the service
   * &mdash; waiting, cancelling, draining, polling state &mdash; is therefore overridden to answer
   * locally, and metrics are served through the batch job's own client.
   */
  static final class ScopedDataflowPipelineJob extends DataflowPipelineJob {
    private final MetricResults scopedMetrics;

    ScopedDataflowPipelineJob(DataflowPipelineJob delegate, String scopePrefix) {
      super(
          /* dataflowClient= */ null,
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
    public State drain() {
      // As for cancel(): draining the shared batch job would abort the other members.
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
