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
package org.apache.beam.runners.samza.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
/**
 * Tests for {@link AsyncDoFnRunner}.
 *
 * <p>Note due to the bug in SAMZA-2761, end-of-stream can cause shutdown while there are still
 * messages in process in asynchronous mode. As a temporary solution, we add more bundles to process
 * in the test inputs.
 */
public class AsyncDoFnRunnerTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.fromOptions(
          PipelineOptionsFactory.fromArgs(
                  "--runner=TestSamzaRunner",
                  "--maxBundleSize=5",
                  "--numThreadsForProcessElement=5")
              .create());

  @Test
  @Ignore("https://github.com/apache/beam/issues/23745")
  public void testSimplePipeline() {
    List<Integer> input = new ArrayList<>();
    for (int i = 1; i < 20; i++) {
      input.add(i);
    }
    PCollection<Integer> square =
        pipeline
            .apply(Create.of(input))
            .apply(Filter.by(x -> x <= 5))
            .apply(MapElements.into(TypeDescriptors.integers()).via(x -> x * x));

    PAssert.that(square).containsInAnyOrder(Arrays.asList(1, 4, 9, 16, 25));

    pipeline.run();
  }

  @Test
  @Ignore("https://github.com/apache/beam/issues/23745")
  public void testPipelineWithState() {
    final List<KV<String, String>> input =
        new ArrayList<>(
            Arrays.asList(
                KV.of("apple", "red"),
                KV.of("banana", "yellow"),
                KV.of("apple", "yellow"),
                KV.of("grape", "purple"),
                KV.of("banana", "yellow")));
    final Map<String, Integer> expectedCount = ImmutableMap.of("apple", 2, "banana", 2, "grape", 1);

    // TODO: remove after SAMZA-2761 fix
    for (int i = 0; i < 20; i++) {
      input.add(KV.of("*", "*"));
    }

    final DoFn<KV<String, String>, KV<String, Integer>> fn =
        new DoFn<KV<String, String>, KV<String, Integer>>() {

          @StateId("cc")
          private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
              StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              ProcessContext c, @StateId("cc") CombiningState<Integer, int[], Integer> countState) {

            if (c.element().getKey().equals("*")) {
              return;
            }

            countState.add(1);
            String key = c.element().getKey();
            int n = countState.read();
            if (n >= expectedCount.get(key)) {
              c.output(KV.of(key, n));
            }
          }
        };

    PCollection<KV<String, Integer>> counts = pipeline.apply(Create.of(input)).apply(ParDo.of(fn));

    PAssert.that(counts)
        .containsInAnyOrder(
            expectedCount.entrySet().stream()
                .map(entry -> KV.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

    pipeline.run();
  }

  @Test
  @Ignore("https://github.com/apache/beam/issues/23745")
  public void testPipelineWithAggregation() {
    final List<KV<String, Long>> input =
        new ArrayList<>(
            Arrays.asList(
                KV.of("apple", 2L),
                KV.of("banana", 5L),
                KV.of("apple", 8L),
                KV.of("grape", 10L),
                KV.of("banana", 5L)));

    // TODO: remove after SAMZA-2761 fix
    for (int i = 0; i < 50; i++) {
      input.add(KV.of("*", 0L));
    }

    PCollection<KV<String, Long>> sums =
        pipeline
            .apply(Create.of(input))
            .apply(Filter.by(x -> !x.getKey().equals("*")))
            .apply(Sum.longsPerKey());

    PAssert.that(sums)
        .containsInAnyOrder(
            Arrays.asList(KV.of("apple", 10L), KV.of("banana", 10L), KV.of("grape", 10L)));

    pipeline.run();
  }

  @Test
  public void testKeyedOutputFutures() {
    // We test the scenario that two elements of the same key needs to be processed in order.
    final DoFnRunner<KV<String, Integer>, Void> doFnRunner = mock(DoFnRunner.class);
    final AtomicInteger prev = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              latch.await();
              WindowedValue<KV<String, Integer>> wv = invocation.getArgument(0);
              Integer val = wv.getValue().getValue();

              // Verify the previous element has been fully processed by checking the prev value
              assertEquals(val - 1, prev.get());

              prev.set(val);
              return null;
            })
        .when(doFnRunner)
        .processElement(any());

    SamzaPipelineOptions options = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    options.setNumThreadsForProcessElement(4);

    final OpEmitter<Void> opEmitter = new OpAdapter.OpEmitterImpl<>();
    final FutureCollector<Void> futureCollector = new FutureCollectorImpl<>();
    futureCollector.prepare();

    final AsyncDoFnRunner<KV<String, Integer>, Void> asyncDoFnRunner =
        AsyncDoFnRunner.create(doFnRunner, opEmitter, futureCollector, true, options);

    final String appleKey = "apple";

    final WindowedValue<KV<String, Integer>> input1 =
        WindowedValue.valueInGlobalWindow(KV.of(appleKey, 1));

    final WindowedValue<KV<String, Integer>> input2 =
        WindowedValue.valueInGlobalWindow(KV.of(appleKey, 2));

    asyncDoFnRunner.processElement(input1);
    asyncDoFnRunner.processElement(input2);
    // Resume input1 process afterwards
    latch.countDown();

    // Waiting for the futures to be resolved
    try {
      futureCollector.finish().toCompletableFuture().get();
    } catch (Exception e) {
      // ignore interruption here.
    }

    // The final val should be the last element value
    assertEquals(2, prev.get());
    // The appleKey in keyedOutputFutures map should be removed
    assertFalse(asyncDoFnRunner.hasOutputFuturesForKey(appleKey));
  }
}
