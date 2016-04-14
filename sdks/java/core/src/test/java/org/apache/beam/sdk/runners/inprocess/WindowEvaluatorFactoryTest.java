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
package org.apache.beam.sdk.runners.inprocess;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.Bound;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link WindowEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class WindowEvaluatorFactoryTest {
  private static final Instant EPOCH = new Instant(0);

  private PCollection<Long> input;
  private WindowEvaluatorFactory factory;

  @Mock private InProcessEvaluationContext evaluationContext;

  private BundleFactory bundleFactory;

  private WindowedValue<Long> first =
      WindowedValue.timestampedValueInGlobalWindow(3L, new Instant(2L));
  private WindowedValue<Long> second =
      WindowedValue.timestampedValueInGlobalWindow(
          Long.valueOf(1L), EPOCH.plus(Duration.standardDays(3)));
  private WindowedValue<Long> third =
      WindowedValue.of(
          Long.valueOf(2L),
          new Instant(-10L),
          new IntervalWindow(new Instant(-100), EPOCH),
          PaneInfo.NO_FIRING);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    TestPipeline p = TestPipeline.create();
    input = p.apply(Create.of(1L, 2L, 3L));

    bundleFactory = InProcessBundleFactory.create();
    factory = new WindowEvaluatorFactory();
  }

  @Test
  public void nullWindowFunSucceeds() throws Exception {
    Bound<Long> transform =
        Window.<Long>triggering(
                AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes();
    PCollection<Long> triggering = input.apply(transform);

    CommittedBundle<Long> inputBundle = createInputBundle();

    UncommittedBundle<Long> outputBundle = createOutputBundle(triggering, inputBundle);

    InProcessTransformResult result = runEvaluator(triggering, inputBundle, transform);

    assertThat(
        Iterables.getOnlyElement(result.getOutputBundles()),
        Matchers.<UncommittedBundle<?>>equalTo(outputBundle));
    CommittedBundle<Long> committed = outputBundle.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(third, first, second));
  }

  @Test
  public void singleWindowFnSucceeds() throws Exception {
    Duration windowDuration = Duration.standardDays(7);
    Bound<Long> transform = Window.<Long>into(FixedWindows.of(windowDuration));
    PCollection<Long> windowed = input.apply(transform);

    CommittedBundle<Long> inputBundle = createInputBundle();

    UncommittedBundle<Long> outputBundle = createOutputBundle(windowed, inputBundle);

    BoundedWindow firstSecondWindow = new IntervalWindow(EPOCH, EPOCH.plus(windowDuration));
    BoundedWindow thirdWindow = new IntervalWindow(EPOCH.minus(windowDuration), EPOCH);

    InProcessTransformResult result = runEvaluator(windowed, inputBundle, transform);

    assertThat(
        Iterables.getOnlyElement(result.getOutputBundles()),
        Matchers.<UncommittedBundle<?>>equalTo(outputBundle));
    CommittedBundle<Long> committed = outputBundle.commit(Instant.now());

    WindowedValue<Long> expectedNewFirst =
        WindowedValue.of(3L, new Instant(2L), firstSecondWindow, PaneInfo.NO_FIRING);
    WindowedValue<Long> expectedNewSecond =
        WindowedValue.of(
            1L, EPOCH.plus(Duration.standardDays(3)), firstSecondWindow, PaneInfo.NO_FIRING);
    WindowedValue<Long> expectedNewThird =
        WindowedValue.of(2L, new Instant(-10L), thirdWindow, PaneInfo.NO_FIRING);
    assertThat(
        committed.getElements(),
        containsInAnyOrder(expectedNewFirst, expectedNewSecond, expectedNewThird));
  }

  @Test
  public void multipleWindowsWindowFnSucceeds() throws Exception {
    Duration windowDuration = Duration.standardDays(6);
    Duration slidingBy = Duration.standardDays(3);
    Bound<Long> transform = Window.into(SlidingWindows.of(windowDuration).every(slidingBy));
    PCollection<Long> windowed = input.apply(transform);

    CommittedBundle<Long> inputBundle = createInputBundle();
    UncommittedBundle<Long> outputBundle = createOutputBundle(windowed, inputBundle);

    InProcessTransformResult result = runEvaluator(windowed, inputBundle, transform);

    assertThat(
        Iterables.getOnlyElement(result.getOutputBundles()),
        Matchers.<UncommittedBundle<?>>equalTo(outputBundle));
    CommittedBundle<Long> committed = outputBundle.commit(Instant.now());

    BoundedWindow w1 = new IntervalWindow(EPOCH, EPOCH.plus(windowDuration));
    BoundedWindow w2 =
        new IntervalWindow(EPOCH.plus(slidingBy), EPOCH.plus(slidingBy).plus(windowDuration));
    BoundedWindow wMinus1 = new IntervalWindow(EPOCH.minus(windowDuration), EPOCH);
    BoundedWindow wMinusSlide =
        new IntervalWindow(EPOCH.minus(windowDuration).plus(slidingBy), EPOCH.plus(slidingBy));

    WindowedValue<Long> expectedFirst =
        WindowedValue.of(
            first.getValue(),
            first.getTimestamp(),
            ImmutableSet.of(w1, wMinusSlide),
            PaneInfo.NO_FIRING);
    WindowedValue<Long> expectedSecond =
        WindowedValue.of(
            second.getValue(), second.getTimestamp(), ImmutableSet.of(w1, w2), PaneInfo.NO_FIRING);
    WindowedValue<Long> expectedThird =
        WindowedValue.of(
            third.getValue(),
            third.getTimestamp(),
            ImmutableSet.of(wMinus1, wMinusSlide),
            PaneInfo.NO_FIRING);

    assertThat(
        committed.getElements(), containsInAnyOrder(expectedFirst, expectedSecond, expectedThird));
  }

  private CommittedBundle<Long> createInputBundle() {
    CommittedBundle<Long> inputBundle =
        bundleFactory
            .createRootBundle(input)
            .add(first)
            .add(second)
            .add(third)
            .commit(Instant.now());
    return inputBundle;
  }

  private UncommittedBundle<Long> createOutputBundle(
      PCollection<Long> output, CommittedBundle<Long> inputBundle) {
    UncommittedBundle<Long> outputBundle = bundleFactory.createBundle(inputBundle, output);
    when(evaluationContext.createBundle(inputBundle, output)).thenReturn(outputBundle);
    return outputBundle;
  }

  private InProcessTransformResult runEvaluator(
      PCollection<Long> windowed,
      CommittedBundle<Long> inputBundle,
      Window.Bound<Long> windowTransform /* Required while Window.Bound is a composite */)
      throws Exception {
    TransformEvaluator<Long> evaluator =
        factory.forApplication(
            AppliedPTransform.of("Window", input, windowed, windowTransform),
            inputBundle,
            evaluationContext);

    evaluator.processElement(first);
    evaluator.processElement(second);
    evaluator.processElement(third);
    InProcessTransformResult result = evaluator.finishBundle();
    return result;
  }
}
