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

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.base.Preconditions;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Tests for {@link View}. See also {@link ParDoTest}, which
 * provides additional coverage since views can only be
 * observed via {@link ParDo}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class ViewTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void testSingletonSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Integer> view = pipeline
        .apply("Create47", Create.of(47))
        .apply(View.<Integer>asSingleton());

    PCollection<Integer> output = pipeline
        .apply("Create123", Create.of(1, 2, 3))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    DataflowAssert.that(output)
        .containsInAnyOrder(47, 47, 47);

    pipeline.run();
  }

  @Test
  public void testEmptySingletonSideInput() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Integer> view = pipeline
        .apply("CreateEmptyIntegers", Create.<Integer>of().withCoder(VarIntCoder.of()))
        .apply(View.<Integer>asSingleton());

    pipeline
        .apply("Create123", Create.of(1, 2, 3))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(NoSuchElementException.class));
    thrown.expectMessage("Empty");
    thrown.expectMessage("PCollection");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  public void testNonSingletonSideInput() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> oneTwoThree = pipeline.apply(Create.<Integer>of(1, 2, 3));
    final PCollectionView<Integer> view = oneTwoThree
        .apply(View.<Integer>asSingleton());

    oneTwoThree
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("PCollection");
    thrown.expectMessage("more than one");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testListSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<List<Integer>> view = pipeline
        .apply("CreateSideInput", Create.of(11, 13, 17, 23))
        .apply(View.<Integer>asList());

    PCollection<Integer> output = pipeline
        .apply("CreateMainInput", Create.of(29, 31))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                Preconditions.checkArgument(c.sideInput(view).size() == 4);
                Preconditions.checkArgument(c.sideInput(view).get(0) == c.sideInput(view).get(0));
                for (Integer i : c.sideInput(view)) {
                  c.output(i);
                }
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder(
        11, 13, 17, 23,
        11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testIterableSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Iterable<Integer>> view = pipeline
        .apply("CreateSideInput", Create.of(11, 13, 17, 23))
        .apply(View.<Integer>asIterable());

    PCollection<Integer> output = pipeline
        .apply("CreateMainInput", Create.of(29, 31))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                for (Integer i : c.sideInput(view)) {
                  c.output(i);
                }
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder(
        11, 13, 17, 23,
        11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultimapSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Map<String, Iterable<Integer>>> view = pipeline
        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
        .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output = pipeline
        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, KV<String, Integer>>() {
              @Override
              public void processElement(ProcessContext c) {
                for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                  c.output(KV.of(c.element(), v));
                }
              }
            }));

    DataflowAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("apple", 2),
                            KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMapSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Map<String, Integer>> view = pipeline
        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
        .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output = pipeline
        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, KV<String, Integer>>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(KV.of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
              }
            }));

    DataflowAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1),
                            KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCombinedMapSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Map<String, Integer>> view = pipeline
        .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 20), KV.of("b", 3)))
        .apply("SumIntegers",
            Combine.perKey(new Sum.SumIntegerFn().<String>asKeyedFn()))
        .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output = pipeline
        .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
        .apply("Output", ParDo.withSideInputs(view).of(
            new DoFn<String, KV<String, Integer>>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(KV.of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
              }
            }));

    DataflowAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 21),
                            KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToFixed() {
    Pipeline p = TestPipeline.create();

    final PCollectionView<Integer> view = p
        .apply("CreateSideInput", Create.timestamped(
            TimestampedValue.of(1, new Instant(1)),
            TimestampedValue.of(2, new Instant(11)),
            TimestampedValue.of(3, new Instant(13))))
        .apply("WindowSideInput", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersGlobally().withoutDefaults())
        .apply(View.<Integer>asSingleton());

    PCollection<String> output = p
        .apply("CreateMainInput", Create.timestamped(
            TimestampedValue.of("A", new Instant(4)),
            TimestampedValue.of("B", new Instant(15)),
            TimestampedValue.of("C", new Instant(7))))
        .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
        .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, String>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.element() + c.sideInput(view));
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder("A1", "B5", "C1");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToGlobal() {
    Pipeline p = TestPipeline.create();

    final PCollectionView<Integer> view = p
        .apply("CreateSideInput", Create.timestamped(
            TimestampedValue.of(1, new Instant(1)),
            TimestampedValue.of(2, new Instant(11)),
            TimestampedValue.of(3, new Instant(13))))
        .apply("WindowSideInput", Window.<Integer>into(new GlobalWindows()))
        .apply(Sum.integersGlobally())
        .apply(View.<Integer>asSingleton());

    PCollection<String> output = p
        .apply("CreateMainInput", Create.timestamped(
            TimestampedValue.of("A", new Instant(4)),
            TimestampedValue.of("B", new Instant(15)),
            TimestampedValue.of("C", new Instant(7))))
        .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
        .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, String>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.element() + c.sideInput(view));
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder("A6", "B6", "C6");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToFixedWithDefault() {
    Pipeline p = TestPipeline.create();

    final PCollectionView<Integer> view = p
        .apply("CreateSideInput", Create.timestamped(
            TimestampedValue.of(2, new Instant(11)),
            TimestampedValue.of(3, new Instant(13))))
        .apply("WindowSideInput", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersGlobally().asSingletonView());

    PCollection<String> output = p
        .apply("CreateMainInput", Create.timestamped(
            TimestampedValue.of("A", new Instant(4)),
            TimestampedValue.of("B", new Instant(15)),
            TimestampedValue.of("C", new Instant(7))))
        .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
        .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, String>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.element() + c.sideInput(view));
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder("A0", "B5", "C0");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSideInputWithNullDefault() {
    Pipeline p = TestPipeline.create();

    final PCollectionView<Void> view = p
        .apply("CreateSideInput", Create.of((Void) null).withCoder(VoidCoder.of()))
        .apply(Combine.globally(new SerializableFunction<Iterable<Void>, Void>() {
                  @Override
                  public Void apply(Iterable<Void> input) {
                    return null;
                  }
                }).asSingletonView());

    PCollection<String> output = p
        .apply("CreateMainInput", Create.of(""))
        .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
            new DoFn<String, String>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.element() + c.sideInput(view));
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder("null");

    p.run();
  }

  @Test
  public void testViewGetName() {
    assertEquals("View.AsSingleton", View.<Integer>asSingleton().getName());
    assertEquals("View.AsIterable", View.<Integer>asIterable().getName());
    assertEquals("View.AsMap", View.<String, Integer>asMap().getName());
    assertEquals("View.AsMultimap", View.<String, Integer>asMultimap().getName());
  }

  private Pipeline createTestBatchRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("someproject");
    options.setStagingLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(null);
    return Pipeline.create(options);
  }

  private Pipeline createTestStreamingRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setStreaming(true);
    options.setProject("someproject");
    options.setStagingLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(null);
    return Pipeline.create(options);
  }

  private Pipeline createTestDirectRunner() {
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    options.setRunner(DirectPipelineRunner.class);
    return Pipeline.create(options);
  }

  private void testViewUnbounded(Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("non-bounded PCollection")));
    pipeline
        .apply(new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
          @Override
          public PCollection<KV<String, Integer>> apply(PBegin input) {
            return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
          }
        })
        .apply(view);
  }

  private void testViewNonmerging(Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("Consumed by GroupByKey")));
    pipeline
        .apply(Create.<KV<String, Integer>>of(KV.of("hello", 5)))
        .apply(Window.<KV<String, Integer>>into(new InvalidWindows<>(
            "Consumed by GroupByKey", FixedWindows.of(Duration.standardHours(1)))))
        .apply(view);
  }

  @Test
  public void testViewUnboundedAsSingletonBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsSingletonStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsSingletonDirect() {
    testViewUnbounded(createTestDirectRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsIterableBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsIterableStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsIterableDirect() {
    testViewUnbounded(createTestDirectRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsListBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsListStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsListDirect() {
    testViewUnbounded(createTestDirectRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsMapBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewUnboundedAsMapStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewUnboundedAsMapDirect() {
    testViewUnbounded(createTestDirectRunner(), View.<String, Integer>asMap());
  }


  @Test
  public void testViewUnboundedAsMultimapBatch() {
    testViewUnbounded(createTestBatchRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewUnboundedAsMultimapStreaming() {
    testViewUnbounded(createTestStreamingRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewUnboundedAsMultimapDirect() {
    testViewUnbounded(createTestDirectRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsSingletonBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsSingletonStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsSingletonDirect() {
    testViewNonmerging(createTestDirectRunner(), View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsIterableBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsIterableStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsIterableDirect() {
    testViewNonmerging(createTestDirectRunner(), View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsListBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsListStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsListDirect() {
    testViewNonmerging(createTestDirectRunner(), View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsMapBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewNonmergingAsMapStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<String, Integer>asMap());
  }

  @Test
  public void testViewNonmergingAsMapDirect() {
    testViewNonmerging(createTestDirectRunner(), View.<String, Integer>asMap());
  }


  @Test
  public void testViewNonmergingAsMultimapBatch() {
    testViewNonmerging(createTestBatchRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsMultimapStreaming() {
    testViewNonmerging(createTestStreamingRunner(), View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsMultimapDirect() {
    testViewNonmerging(createTestDirectRunner(), View.<String, Integer>asMultimap());
  }
}
