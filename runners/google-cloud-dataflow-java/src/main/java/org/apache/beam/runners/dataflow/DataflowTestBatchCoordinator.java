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

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatchers;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates merging concurrent batch {@code TestPipeline} executions within a JVM into a single
 * Dataflow job per batch, with scoped {@link PAssert} and {@link MetricResults} verification and
 * automatic standalone fallback if a merged batch fails.
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

  private static final AtomicInteger BATCH_COUNTER = new AtomicInteger(1);
  private static final ConcurrentHashMap<BatchKey, BatchCollector> COLLECTORS =
      new ConcurrentHashMap<>();

  static boolean isEligibleForBatching(Pipeline pipeline, TestDataflowPipelineOptions options) {
    Boolean optionEnabled = options.getEnableTestBatching();
    boolean enabled =
        optionEnabled != null ? optionEnabled : Boolean.getBoolean(TEST_BATCHING_PROPERTY);
    if (!enabled) {
      return false;
    }
    if (getMaxBatchSize(options) <= 1) {
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

  private static int getMaxBatchSize(TestDataflowPipelineOptions options) {
    if (options.getTestBatchMaxSize() > 0) {
      return options.getTestBatchMaxSize();
    }
    int defaultSize = Integer.getInteger(VALIDATES_RUNNER_THREADS_PROPERTY, DEFAULT_BATCH_MAX_SIZE);
    return Integer.getInteger(TEST_BATCH_MAX_SIZE_PROPERTY, Math.max(1, defaultSize));
  }

  private static long getBatchWindowMs(TestDataflowPipelineOptions options) {
    if (options.getTestBatchWindowMs() > 0) {
      return options.getTestBatchWindowMs();
    }
    return Long.getLong(TEST_BATCH_WINDOW_MS_PROPERTY, DEFAULT_BATCH_WINDOW_MS);
  }

  static DataflowPipelineJob runInBatch(
      Pipeline pipeline,
      TestDataflowPipelineOptions options,
      TestDataflowRunner runner,
      DataflowRunner delegateRunner) {
    PendingItem item = new PendingItem(pipeline, options, runner, delegateRunner);
    BatchKey key = BatchKey.fromOptions(options);
    BatchCollector collector = COLLECTORS.computeIfAbsent(key, k -> new BatchCollector());
    return collector.submit(item, getMaxBatchSize(options), getBatchWindowMs(options));
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

  private static final class BatchKey {
    private final @Nullable String project;
    private final @Nullable String region;
    private final boolean streaming;
    private final List<String> experiments;

    private BatchKey(
        @Nullable String project,
        @Nullable String region,
        boolean streaming,
        @Nullable List<String> experiments) {
      this.project = project;
      this.region = region;
      this.streaming = streaming;
      this.experiments =
          experiments == null ? Collections.emptyList() : ImmutableList.copyOf(experiments);
    }

    static BatchKey fromOptions(TestDataflowPipelineOptions options) {
      return new BatchKey(
          options.getProject(),
          options.getRegion(),
          options.isStreaming(),
          options.getExperiments());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BatchKey)) {
        return false;
      }
      BatchKey that = (BatchKey) o;
      return streaming == that.streaming
          && Objects.equals(project, that.project)
          && Objects.equals(region, that.region)
          && Objects.equals(experiments, that.experiments);
    }

    @Override
    public int hashCode() {
      return Objects.hash(project, region, streaming, experiments);
    }
  }

  static final class PendingItem {
    final Pipeline pipeline;
    final TestDataflowPipelineOptions options;
    final TestDataflowRunner runner;
    final DataflowRunner delegateRunner;
    final int expectedAssertions;
    final Runnable restoreSnapshot;
    final CompletableFuture<DataflowPipelineJob> resultFuture = new CompletableFuture<>();

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
  }

  private static final class ActiveBatch {
    final List<PendingItem> items = new ArrayList<>();
    boolean closed = false;
  }

  private static final class BatchCollector {
    private final Object lock = new Object();
    private @Nullable ActiveBatch currentBatch = null;

    DataflowPipelineJob submit(PendingItem item, int maxBatchSize, long batchWindowMs) {
      List<PendingItem> batchToExecute = null;
      synchronized (lock) {
        if (currentBatch == null || currentBatch.closed) {
          ActiveBatch myBatch = new ActiveBatch();
          myBatch.items.add(item);
          currentBatch = myBatch;

          long deadlineMs = System.currentTimeMillis() + batchWindowMs;
          while (!myBatch.closed && myBatch.items.size() < maxBatchSize) {
            long remainingMs = deadlineMs - System.currentTimeMillis();
            if (remainingMs <= 0) {
              break;
            }
            try {
              lock.wait(remainingMs);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          myBatch.closed = true;
          if (currentBatch == myBatch) {
            currentBatch = null;
          }
          batchToExecute = new ArrayList<>(myBatch.items);
        } else {
          ActiveBatch joinedBatch = currentBatch;
          joinedBatch.items.add(item);
          if (joinedBatch.items.size() >= maxBatchSize) {
            joinedBatch.closed = true;
            currentBatch = null;
            lock.notifyAll();
          }
        }
      }

      if (batchToExecute != null) {
        executeBatch(batchToExecute);
      }

      try {
        return item.resultFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new RuntimeException(cause != null ? cause : e);
      }
    }
  }

  @VisibleForTesting
  static void executeBatch(List<PendingItem> items) {
    if (items.isEmpty()) {
      return;
    }
    if (items.size() == 1) {
      runStandaloneIntoFuture(items.get(0));
      return;
    }

    int batchNumber = BATCH_COUNTER.getAndIncrement();
    LOG.info(
        "Merging {} ValidatesRunner test pipelines into Dataflow batch #{}",
        items.size(),
        batchNumber);

    List<Pipeline> memberPipelines = new ArrayList<>(items.size());
    for (int i = 0; i < items.size(); i++) {
      String scopePrefix = "t" + i;
      Pipeline member = items.get(i).pipeline;
      member.setRootNamePrefix(scopePrefix);
      memberPipelines.add(member);
    }

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
    boolean batchJobDone = false;
    JobMetrics batchMetrics = null;
    try {
      batchJob = leader.delegateRunner.run(compositePipeline);
      LOG.info(
          "Submitted merged Dataflow job {} for batch #{} ({} tests)",
          batchJob.getJobId(),
          batchNumber,
          items.size());
      batchJobDone = leader.runner.waitForBatchJobTermination(batchJob);
      if (batchJobDone) {
        batchMetrics = leader.runner.getJobMetrics(batchJob);
      }
    } catch (Throwable t) {
      LOG.warn(
          "Merged Dataflow batch #{} failed during submission or execution; falling back to"
              + " standalone execution for {} tests.",
          batchNumber,
          items.size(),
          t);
    } finally {
      leader.options.setJobName(originalLeaderJobName);
    }

    List<PendingItem> fallbackItems = new ArrayList<>();
    if (batchJob != null && batchJobDone) {
      for (int i = 0; i < items.size(); i++) {
        PendingItem item = items.get(i);
        String scopePrefix = "t" + i;
        if (checkScopedPAssertSuccess(
            batchJob, batchMetrics, scopePrefix, item.expectedAssertions)) {
          item.resultFuture.complete(new ScopedDataflowPipelineJob(batchJob, scopePrefix));
        } else {
          LOG.warn(
              "Test {} (scope {}) did not pass scoped PAssert check in merged job {}; re-running"
                  + " standalone.",
              item.options.getAppName(),
              scopePrefix,
              batchJob.getJobId());
          fallbackItems.add(item);
        }
      }
    } else {
      fallbackItems.addAll(items);
    }

    if (!fallbackItems.isEmpty()) {
      runFallbackItemsInParallel(fallbackItems);
    }
  }

  private static void runFallbackItemsInParallel(List<PendingItem> fallbackItems) {
    List<Thread> fallbackThreads = new ArrayList<>(fallbackItems.size());
    for (int i = 0; i < fallbackItems.size(); i++) {
      final PendingItem item = fallbackItems.get(i);
      item.restoreSnapshot.run();
      if (i == fallbackItems.size() - 1) {
        // Run the last item on the current (leader) thread.
        runStandaloneIntoFuture(item);
      } else {
        Thread thread =
            new Thread(
                () -> runStandaloneIntoFuture(item),
                "beam-batch-fallback-" + item.options.getAppName());
        thread.setDaemon(true);
        thread.start();
        fallbackThreads.add(thread);
      }
    }
    for (Thread thread : fallbackThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void runStandaloneIntoFuture(PendingItem item) {
    try {
      DataflowPipelineJob job = item.runner.runStandalone(item.pipeline, item.delegateRunner);
      item.resultFuture.complete(job);
    } catch (Throwable t) {
      item.resultFuture.completeExceptionally(t);
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
   */
  static final class ScopedDataflowPipelineJob extends DataflowPipelineJob {
    private final DataflowPipelineJob delegate;
    private final MetricResults scopedMetrics;

    ScopedDataflowPipelineJob(DataflowPipelineJob delegate, String scopePrefix) {
      super(
          null,
          delegate.getJobId(),
          delegate.getDataflowOptions(),
          delegate.getTransformStepNames(),
          delegate.getPipelineProto());
      this.delegate = delegate;
      this.scopedMetrics = new ScopedMetricResults(delegate, scopePrefix);
    }

    @Override
    public State getState() {
      return delegate.getState();
    }

    @Override
    public @Nullable State waitUntilFinish() {
      return delegate.getState();
    }

    @Override
    public @Nullable State waitUntilFinish(Duration duration) {
      return delegate.getState();
    }

    @Override
    public @Nullable State waitUntilFinish(
        Duration duration, MonitoringUtil.JobMessagesHandler messageHandler) {
      return delegate.getState();
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
