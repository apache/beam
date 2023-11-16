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
package org.apache.beam.runners.direct;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.rules.RuleChain.outerRule;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.metrics.MetricsPusherTest;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ThreadLeakTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.LinkedListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/** Validates some basic behavior for the ExecutorServiceParallelExecutor. */
public class ExecutorServiceParallelExecutorTest {

  private static final long NUM_ELEMENTS = 1000L;

  private final TestPipeline pipeline = TestPipeline.create();
  private final TestRule threadLeakTracker = new ThreadLeakTracker();

  @Rule public final TestRule execution = outerRule(pipeline).around(threadLeakTracker);

  @Rule public final TestName testName = new TestName();

  @Test
  @Ignore("https://github.com/apache/beam/issues/18950 Test reliably fails.")
  public void ensureMetricsThreadDoesntLeak() throws ExecutionException, InterruptedException {
    final DirectGraph graph =
        DirectGraph.create(
            emptyMap(), emptyMap(), LinkedListMultimap.create(), emptySet(), emptyMap());
    final ExecutorService metricsExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("dontleak_" + getClass().getName() + "#" + testName.getMethodName())
                .build());

    // fake a metrics usage
    metricsExecutorService.submit(() -> {}).get();

    final EvaluationContext context =
        EvaluationContext.create(
            MockClock.fromInstant(Instant.now()),
            CloningBundleFactory.create(),
            graph,
            emptySet(),
            metricsExecutorService);
    ExecutorServiceParallelExecutor.create(
            2,
            TransformEvaluatorRegistry.javaSdkNativeRegistry(
                context, PipelineOptionsFactory.create().as(DirectOptions.class)),
            emptyMap(),
            context,
            metricsExecutorService)
        .stop();
    try {
      metricsExecutorService.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Test
  @Ignore("https://github.com/apache/beam/issues/18950 Test reliably fails.")
  public void testNoThreadsLeakInPipelineExecution() {
    pipeline.apply(GenerateSequence.from(0).to(NUM_ELEMENTS)).apply(ParDo.of(new CountingDoFn()));
    pipeline.run();
  }

  private static class CountingDoFn extends DoFn<Long, Long> {
    private final Counter counter = Metrics.counter(MetricsPusherTest.class, "counter");

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        counter.inc();
        context.output(context.element());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
