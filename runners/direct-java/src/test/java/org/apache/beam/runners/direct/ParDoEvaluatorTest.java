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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentitySideInputWindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ParDoEvaluator}. */
@RunWith(JUnit4.class)
public class ParDoEvaluatorTest {
  @Mock private EvaluationContext evaluationContext;
  private PCollection<Integer> inputPc;
  private TupleTag<Integer> mainOutputTag;
  private List<TupleTag<?>> additionalOutputTags;
  private BundleFactory bundleFactory;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    inputPc = p.apply(Create.of(1, 2, 3));
    mainOutputTag = new TupleTag<Integer>() {};
    additionalOutputTags = TupleTagList.empty().getAll();

    bundleFactory = ImmutableListBundleFactory.create();
  }

  @Test
  public void sideInputsNotReadyResultHasUnprocessedElements() {
    PCollectionView<Integer> singletonView =
        inputPc
            .apply(Window.into(new IdentitySideInputWindowFn()))
            .apply(View.<Integer>asSingleton().withDefaultValue(0));
    RecorderFn fn = new RecorderFn(singletonView);
    PCollection<Integer> output = inputPc.apply(ParDo.of(fn).withSideInputs(singletonView));

    UncommittedBundle<Integer> outputBundle = bundleFactory.createBundle(output);
    when(evaluationContext.createBundle(output)).thenReturn(outputBundle);

    ParDoEvaluator<Integer> evaluator = createEvaluator(singletonView, fn, inputPc, output);

    IntervalWindow nonGlobalWindow = new IntervalWindow(new Instant(0), new Instant(10_000L));
    WindowedValue<Integer> first = WindowedValue.valueInGlobalWindow(3);
    WindowedValue<Integer> second =
        WindowedValue.of(2, new Instant(1234L), nonGlobalWindow, PaneInfo.NO_FIRING);
    WindowedValue<Integer> third =
        WindowedValue.of(
            1,
            new Instant(2468L),
            ImmutableList.of(nonGlobalWindow, GlobalWindow.INSTANCE),
            PaneInfo.NO_FIRING);

    evaluator.processElement(first);
    evaluator.processElement(second);
    evaluator.processElement(third);
    TransformResult<Integer> result = evaluator.finishBundle();

    assertThat(
        result.getUnprocessedElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(
            second, WindowedValue.of(1, new Instant(2468L), nonGlobalWindow, PaneInfo.NO_FIRING)));
    assertThat(result.getOutputBundles(), Matchers.contains(outputBundle));
    assertThat(fn.processed, containsInAnyOrder(1, 3));
    assertThat(
        Iterables.getOnlyElement(result.getOutputBundles()).commit(Instant.now()).getElements(),
        containsInAnyOrder(
            first.withValue(8),
            WindowedValue.timestampedValueInGlobalWindow(6, new Instant(2468L))));
  }

  private ParDoEvaluator<Integer> createEvaluator(
      PCollectionView<Integer> singletonView,
      RecorderFn fn,
      PCollection<Integer> input,
      PCollection<Integer> output) {
    when(evaluationContext.createSideInputReader(ImmutableList.of(singletonView)))
        .thenReturn(new ReadyInGlobalWindowReader());
    DirectExecutionContext executionContext = mock(DirectExecutionContext.class);
    DirectStepContext stepContext = mock(DirectStepContext.class);
    when(executionContext.getStepContext(Mockito.any(String.class))).thenReturn(stepContext);
    when(stepContext.getTimerUpdate()).thenReturn(TimerUpdate.empty());
    when(evaluationContext.getExecutionContext(
            Mockito.any(AppliedPTransform.class), Mockito.any(StructuralKey.class)))
        .thenReturn(executionContext);

    DirectGraphs.performDirectOverrides(p);
    @SuppressWarnings("unchecked")
    AppliedPTransform<PCollection<Integer>, ?, ?> transform =
        (AppliedPTransform<PCollection<Integer>, ?, ?>) DirectGraphs.getProducer(output);
    return ParDoEvaluator.create(
        evaluationContext,
        PipelineOptionsFactory.create(),
        stepContext,
        transform,
        input.getCoder(),
        input.getWindowingStrategy(),
        fn,
        null /* key */,
        ImmutableList.of(singletonView),
        mainOutputTag,
        additionalOutputTags,
        ImmutableMap.of(mainOutputTag, output),
        DoFnSchemaInformation.create(),
        Collections.emptyMap(),
        ParDoEvaluator.defaultRunnerFactory());
  }

  private static class RecorderFn extends DoFn<Integer, Integer> {
    private Collection<Integer> processed;
    private final PCollectionView<Integer> view;

    public RecorderFn(PCollectionView<Integer> view) {
      processed = new ArrayList<>();
      this.view = view;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      processed.add(c.element());
      c.output(c.element() + c.sideInput(view));
    }
  }

  private static class ReadyInGlobalWindowReader implements ReadyCheckingSideInputReader {
    @Override
    public @Nullable <T> T get(PCollectionView<T> view, BoundedWindow window) {
      if (window.equals(GlobalWindow.INSTANCE)) {
        return (T) (Integer) 5;
      }
      fail("Should only call get in the Global Window, others are not ready");
      throw new AssertionError("Unreachable");
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return true;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isReady(PCollectionView<?> view, BoundedWindow window) {
      return window.equals(GlobalWindow.INSTANCE);
    }
  }
}
