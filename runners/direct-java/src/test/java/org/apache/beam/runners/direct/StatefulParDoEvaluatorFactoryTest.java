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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory.StatefulParDo;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link StatefulParDoEvaluatorFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class StatefulParDoEvaluatorFactoryTest implements Serializable {
  @Mock private transient EvaluationContext mockEvaluationContext;
  @Mock private transient DirectExecutionContext mockExecutionContext;
  @Mock private transient DirectExecutionContext.DirectStepContext mockStepContext;
  @Mock private transient ReadyCheckingSideInputReader mockSideInputReader;
  @Mock private transient UncommittedBundle<Integer> mockUncommittedBundle;

  private static final String KEY = "any-key";
  private final transient PipelineOptions options = PipelineOptionsFactory.create();
  private final transient StateInternals stateInternals =
      CopyOnAccessInMemoryStateInternals.<Object>withUnderlying(KEY, null);
  private final transient DirectTimerInternals timerInternals =
      DirectTimerInternals.create(
          MockClock.fromInstant(Instant.now()),
          Mockito.mock(TransformWatermarks.class),
          TimerUpdate.builder(StructuralKey.of(KEY, StringUtf8Coder.of())));

  private static final BundleFactory BUNDLE_FACTORY = ImmutableListBundleFactory.create();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when((StateInternals) mockStepContext.stateInternals()).thenReturn(stateInternals);
    when(mockStepContext.timerInternals()).thenReturn(timerInternals);
    when(mockEvaluationContext.createSideInputReader(anyList()))
        .thenReturn(
            SideInputContainer.create(mockEvaluationContext, Collections.emptyList())
                .createReaderForViews(Collections.emptyList()));
  }

  /**
   * A test that explicitly delays a side input so that the main input will have to be reprocessed,
   * testing that {@code finishBundle()} re-assembles the GBK outputs correctly.
   */
  @Test
  public void testUnprocessedElements() throws Exception {
    // To test the factory, first we set up a pipeline and then we use the constructed
    // pipeline to create the right parameters to pass to the factory

    final String stateId = "my-state-id";

    // For consistency, window it into FixedWindows. Actually we will fabricate an input bundle.
    PCollection<KV<String, Integer>> mainInput =
        pipeline
            .apply(Create.of(KV.of("hello", 1), KV.of("hello", 2)))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))));

    final PCollectionView<List<Integer>> sideInput =
        pipeline
            .apply("Create side input", Create.of(42))
            .apply("Window side input", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply("View side input", View.asList());

    TupleTag<Integer> mainOutput = new TupleTag<>();
    PCollection<Integer> produced =
        mainInput
            .apply(
                new ParDoMultiOverrideFactory.GbkThenStatefulParDo<>(
                    new DoFn<KV<String, Integer>, Integer>() {

                      @StateId(stateId)
                      private final StateSpec<ValueState<String>> spec =
                          StateSpecs.value(StringUtf8Coder.of());

                      @ProcessElement
                      public void process(ProcessContext c) {}
                    },
                    mainOutput,
                    TupleTagList.empty(),
                    Collections.singletonList(sideInput),
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap()))
            .get(mainOutput)
            .setCoder(VarIntCoder.of());

    StatefulParDoEvaluatorFactory<String, Integer, Integer> factory =
        new StatefulParDoEvaluatorFactory<>(mockEvaluationContext, options);

    // This will be the stateful ParDo from the expansion
    AppliedPTransform<
            PCollection<KeyedWorkItem<String, KV<String, Integer>>>,
            PCollectionTuple,
            StatefulParDo<String, Integer, Integer>>
        producingTransform = (AppliedPTransform) DirectGraphs.getProducer(produced);

    // Then there will be a digging down to the step context to get the state internals
    when(mockEvaluationContext.getExecutionContext(
            eq(producingTransform), Mockito.<StructuralKey>any()))
        .thenReturn(mockExecutionContext);
    when(mockExecutionContext.getStepContext(any())).thenReturn(mockStepContext);
    when(mockEvaluationContext.createBundle(Matchers.<PCollection<Integer>>any()))
        .thenReturn(mockUncommittedBundle);
    when(mockStepContext.getTimerUpdate()).thenReturn(TimerUpdate.empty());

    // And digging to check whether the window is ready
    when(mockEvaluationContext.createSideInputReader(anyList())).thenReturn(mockSideInputReader);
    when(mockSideInputReader.isReady(Matchers.any(), Matchers.any())).thenReturn(false);

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(9));

    // A single bundle with some elements in the global window; it should register cleanup for the
    // global window state merely by having the evaluator created. The cleanup logic does not
    // depend on the window.
    String key = "hello";
    WindowedValue<KV<String, Integer>> firstKv =
        WindowedValue.of(KV.of(key, 1), new Instant(3), firstWindow, PaneInfo.NO_FIRING);

    WindowedValue<KeyedWorkItem<String, KV<String, Integer>>> gbkOutputElement =
        firstKv.withValue(
            KeyedWorkItems.elementsWorkItem(
                "hello",
                ImmutableList.of(
                    firstKv,
                    firstKv.withValue(KV.of(key, 13)),
                    firstKv.withValue(KV.of(key, 15)))));

    CommittedBundle<KeyedWorkItem<String, KV<String, Integer>>> inputBundle =
        BUNDLE_FACTORY
            .createBundle(
                (PCollection<KeyedWorkItem<String, KV<String, Integer>>>)
                    Iterables.getOnlyElement(
                        TransformInputs.nonAdditionalInputs(producingTransform)))
            .add(gbkOutputElement)
            .commit(Instant.now());
    TransformEvaluator<KeyedWorkItem<String, KV<String, Integer>>> evaluator =
        factory.forApplication(producingTransform, inputBundle);

    evaluator.processElement(gbkOutputElement);

    // This should push back every element as a KV<String, Iterable<Integer>>
    // in the appropriate window. Since the keys are equal they are single-threaded
    TransformResult<KeyedWorkItem<String, KV<String, Integer>>> result = evaluator.finishBundle();

    List<Integer> pushedBackInts = new ArrayList<>();

    for (WindowedValue<? extends KeyedWorkItem<String, KV<String, Integer>>> unprocessedElement :
        result.getUnprocessedElements()) {

      assertThat(
          Iterables.getOnlyElement(unprocessedElement.getWindows()),
          equalTo((BoundedWindow) firstWindow));

      assertThat(unprocessedElement.getValue().key(), equalTo("hello"));
      for (WindowedValue<KV<String, Integer>> windowedKv :
          unprocessedElement.getValue().elementsIterable()) {
        pushedBackInts.add(windowedKv.getValue().getValue());
      }
    }
    assertThat(pushedBackInts, containsInAnyOrder(1, 13, 15));
  }

  @Test
  public void testRequiresTimeSortedInput() {
    Instant now = Instant.ofEpochMilli(0);
    PCollection<KV<String, Integer>> input =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("", 1), now.plus(Duration.millis(2))),
                TimestampedValue.of(KV.of("", 2), now.plus(Duration.millis(1))),
                TimestampedValue.of(KV.of("", 3), now)));
    PCollection<String> result = input.apply(ParDo.of(statefulConcat()));
    PAssert.that(result).containsInAnyOrder("3", "3:2", "3:2:1");
    pipeline.run();
  }

  @Test
  public void testRequiresTimeSortedInputWithLateData() {
    Instant now = Instant.ofEpochMilli(0);
    PCollection<KV<String, Integer>> input =
        pipeline.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
                .addElements(TimestampedValue.of(KV.of("", 1), now.plus(Duration.millis(2))))
                .addElements(TimestampedValue.of(KV.of("", 2), now.plus(Duration.millis(1))))
                .advanceWatermarkTo(now.plus(Duration.millis(1)))
                .addElements(TimestampedValue.of(KV.of("", 3), now))
                .advanceWatermarkToInfinity());
    PCollection<String> result = input.apply(ParDo.of(statefulConcat()));
    PAssert.that(result).containsInAnyOrder("2", "2:1");
    pipeline.run();
  }

  @Test
  public void testRequiresTimeSortedInputWithLateDataAndAllowedLateness() {
    Instant now = Instant.ofEpochMilli(0);
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(
                TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
                    .addElements(TimestampedValue.of(KV.of("", 1), now.plus(Duration.millis(2))))
                    .addElements(TimestampedValue.of(KV.of("", 2), now.plus(Duration.millis(1))))
                    .advanceWatermarkTo(now.plus(Duration.millis(1)))
                    .addElements(TimestampedValue.of(KV.of("", 3), now))
                    .advanceWatermarkToInfinity())
            .apply(
                Window.<KV<String, Integer>>into(new GlobalWindows())
                    .withAllowedLateness(Duration.millis(2)));
    PCollection<String> result = input.apply(ParDo.of(statefulConcat()));
    PAssert.that(result).containsInAnyOrder("3", "3:2", "3:2:1");
    pipeline.run();
  }

  private static DoFn<KV<String, Integer>, String> statefulConcat() {

    final String stateId = "sum";

    return new DoFn<KV<String, Integer>, String>() {

      @StateId(stateId)
      final StateSpec<ValueState<String>> stateSpec = StateSpecs.value();

      @ProcessElement
      @RequiresTimeSortedInput
      public void processElement(
          ProcessContext context, @StateId(stateId) ValueState<String> state) {
        String current = MoreObjects.firstNonNull(state.read(), "");
        if (!current.isEmpty()) {
          current += ":";
        }
        current += context.element().getValue();
        context.output(current);
        state.write(current);
      }
    };
  }
}
