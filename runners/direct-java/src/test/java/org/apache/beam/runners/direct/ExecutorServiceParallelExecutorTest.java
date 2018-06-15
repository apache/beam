/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
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
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/** Validates some basic behavior for the ExecutorServiceParallelExecutor. */
public class ExecutorServiceParallelExecutorTest {

  private static final long NUM_ELEMENTS = 1000L;
  @Rule public final TestName testName = new TestName();
  private final TestPipeline pipeline = TestPipeline.create();
  private final TestRule threadLeakTracker = new ThreadLeakTracker();
  @Rule public final TestRule execution = outerRule(pipeline).around(threadLeakTracker);

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-4088 Test reliably fails.")
  public void ensureMetricsThreadDoesntLeak() {
    final DirectGraph graph =
        DirectGraph.create(
            emptyMap(), emptyMap(), LinkedListMultimap.create(), emptySet(), emptyMap());
    final InstrumentedThreadPoolExecutor metricsExecutorService =
        new InstrumentedThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue(),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("dontleak_" + getClass().getName() + "#" + testName.getMethodName())
                .build());

    // fake a metrics usage
    metricsExecutorService.submit(() -> {});

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
    if (!metricsExecutorService.isTerminated()) {
      if (metricsExecutorService.isTerminating()) {
        throw new RuntimeException(
            String.format(
                "metricsExecutorService is terminating but still has %s threads. History is: %s",
                metricsExecutorService.getPoolSize(), metricsExecutorService.showMessages()));
      } else {
        throw new RuntimeException(
            String.format(
                "metricsExecutorService should be terminating. History is: %s",
                metricsExecutorService.showMessages()));
      }
    }
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-4088 Test reliably fails.")
  public void testNoThreadsLeakInPipelineExecution() {
    pipeline.apply(GenerateSequence.from(0).to(NUM_ELEMENTS)).apply(ParDo.of(new CountingDoFn()));
    pipeline.run();
  }

  private static class InstrumentedThreadPoolExecutor extends ThreadPoolExecutor {

    private List<String> messages = new ArrayList<>();

    private InstrumentedThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    public void shutdown() {
      messages.add(String.format("shutdown %s " + Instant.now()));
      super.shutdown();
    }

    @Override
    public void execute(Runnable command) {
      super.execute(command);
      messages.add(String.format("after execute %s " + Instant.now()));
    }

    @Override
    protected void terminated() {
      messages.add(String.format("terminated %s " + Instant.now()));
      super.terminated();
    }

    public String showMessages() {
      return Arrays.toString(messages.toArray());
    }
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
