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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.AggregatorRetrievalException;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.common.worker.ReadOperation;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Test framework for verifying that particular elements of a collection can be processed
 * in parallel via dynamic work rebalancing, regardless of the initial work partitioning.
 * The motivating use case is verifying the quality of an implementation of
 * {@link com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader#splitAtFraction}
 * (correctness in terms of data consistency can already be verified by methods in
 * {@link com.google.cloud.dataflow.sdk.testing.SourceTestUtils}).
 *
 * <p>This test works by blocking at a pre-selected set of sentinel values and making sure
 * the work gets split up such that a thread eventually gets allocated to each of them.
 */
public class VerifyDynamicWorkRebalancing {

  private static final Logger LOG = LoggerFactory.getLogger(VerifyDynamicWorkRebalancing.class);

  private VerifyDynamicWorkRebalancing() {
    // Do not instantiate.
  }

  /**
   * Reads a source and attempts to dynamically rebalance work to bundles each containing a single
   * one of the sentinel values using the dataflow runner.  It does this by waiting at each
   * sentinel value and letting the service dynamically split the work into bundles until
   * the sentinels are completely separated.
   *
   * <p>Sentinels should be chosen such that the source's inherent parallelization allows them
   * to be separated. For example, in a simple record-based file format, they can be chosen
   * arbitrarily (e.g. every record is a sentinel), however e.g. in a block-based file format
   * where parallelization can only happen down to blocks, but not down to individual records,
   * sentinels must be in different blocks. However, there should be not too many sentinels,
   * because the test naturally requires at least as many threads (possibly via autoscaling)
   * as there are sentinels to complete successfully.
   *
   * @param source a source PTransform producing the PCollection to be split
   * @param sentinels a collection of elements that should be separable in the source
   * @param nonSentinelSleepMsec how long each non-sentinel element should take to process
   */
  public static <T> void run(
      PTransform<PBegin, PCollection<T>> source,
      Collection<T> sentinels,
      long nonSentinelSleepMsec) {
    runWithPipeline(configurePipeline(sentinels.size(), nonSentinelSleepMsec),
                    source, sentinels, nonSentinelSleepMsec);
  }

  /**
   * Like {@link #run}, but uses a pre-configured pipeline.
   */
  public static <T> void runWithPipeline(
      Pipeline p,
      PTransform<PBegin, PCollection<T>> source,
      Collection<T> sentinels,
      long nonSentinelSleepMsec) {
    HangOnSentinels<T> hangingDoFn = new HangOnSentinels<T>(sentinels, nonSentinelSleepMsec);
    p.apply(source).apply(ParDo.of(hangingDoFn));
    PipelineResult result = p.run();
    long start = System.currentTimeMillis();
    try {
      int seenSentinels;
      do {
        seenSentinels =
            Iterables.getOnlyElement(
                result.getAggregatorValues(hangingDoFn.sentinelCounter).getValues(), 0);
        int seenNonSentinels =
            Iterables.getOnlyElement(
                result.getAggregatorValues(hangingDoFn.nonSentinelCounter).getValues(), 0);
        LOG.info("Seen {} sentinels, {} non-sentinels so far.", seenSentinels, seenNonSentinels);
        sleep(1000);
      } while (seenSentinels < sentinels.size());
      LOG.info("Took {} ms to separate all the sentinels.", System.currentTimeMillis() - start);
      LOG.info("Canceling...");
      ((DataflowPipelineJob) result).cancel();
      LOG.info("Done.");
    } catch (AggregatorRetrievalException exn) {
      throw new RuntimeException(exn);
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
  }

  private static Pipeline configurePipeline(int sentinelCount, long nonSentinelSleepMsec) {
    if (TestPipeline.isIntegrationTest()) {
      // The existing implementation updates the progress at this rate, but never while a data item
      // (e.g. a sentinel value) is being processed, so if non-sentinel sleeps are smaller than
      // the progress update period, that can lead to reporting progress a few items behind,
      // leading to the service making stale split suggestions which will be refused by the worker,
      // and the sentinel will not be separated.
      // TODO: fix progress reporting and dynamic work rebalancing to work well regardless of this
      // condition.
      Preconditions.checkArgument(
          nonSentinelSleepMsec > ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS);
      // Does not work in (single-threaded) direct runner.  Also, we can't use TestPipeline.create()
      // directly as we need a handle on the job (to monitor and cancel it) while it's running.
      PipelineOptions options = TestPipeline.getPipelineOptions();
      options
          .as(DataflowPipelineDebugOptions.class)
          .setNumberOfWorkerHarnessThreads(sentinelCount);
      // Enable autoscaling so the system will scale up to enough workers to trigger dynamic
      // work rebalancing.
      options
          .as(DataflowPipelineWorkerPoolOptions.class)
          .setAutoscalingAlgorithm(
              DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);
      return new TestPipeline(DataflowPipelineRunner.fromOptions(options), options);
    } else {
      // Support for other runners could be added here.
      throw new IllegalArgumentException("Unsupported for this runner.");
    }
  }

  private static class HangOnSentinels<T> extends DoFn<T, Void> {

    private Collection<T> sentinels;
    private final long nonSentinelSleepMsec;

    public final Aggregator<Integer, Integer> sentinelCounter =
        createAggregator("sentinels", new Sum.SumIntegerFn());
    public final Aggregator<Integer, Integer> nonSentinelCounter =
        createAggregator("nonSentinels", new Sum.SumIntegerFn());

    public HangOnSentinels(Collection<T> sentinels, long nonSentinelSleepMsec) {
      this.sentinels = sentinels;
      this.nonSentinelSleepMsec = nonSentinelSleepMsec;
    }

    public void processElement(ProcessContext c) {
      if (sentinels.contains(c.element())) {
        sentinelCounter.addValue(1);
        while (true) {
          LOG.info("Waiting at sentinel {}.", c.element());
          sleep(10000);
        }
      } else {
        nonSentinelCounter.addValue(1);
        sleep(nonSentinelSleepMsec);
      }
    }
  }

  private static void sleep(long slowdownMsec) {
    try {
      Thread.sleep(slowdownMsec);
    } catch (InterruptedException exn) {
      throw new RuntimeException(exn);
    }
  }
}
