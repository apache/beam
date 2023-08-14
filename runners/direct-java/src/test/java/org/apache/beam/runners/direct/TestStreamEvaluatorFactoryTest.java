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
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DirectTestStream;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.TestClock;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.TestStreamIndex;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TestStreamEvaluatorFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
})
public class TestStreamEvaluatorFactoryTest {
  private TestStreamEvaluatorFactory factory;
  private BundleFactory bundleFactory;
  private EvaluationContext context;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  private DirectRunner runner;

  @Before
  public void setup() {
    context = mock(EvaluationContext.class);
    runner = DirectRunner.fromOptions(TestPipeline.testingPipelineOptions());
    factory = new TestStreamEvaluatorFactory(context);
    bundleFactory = ImmutableListBundleFactory.create();
  }

  /** Demonstrates that returned evaluators produce elements in sequence. */
  @Test
  public void producesElementsInSequence() throws Exception {
    TestStream<Integer> testStream =
        TestStream.create(VarIntCoder.of())
            .addElements(1, 2, 3)
            .advanceWatermarkTo(new Instant(0))
            .addElements(
                TimestampedValue.atMinimumTimestamp(4),
                TimestampedValue.atMinimumTimestamp(5),
                TimestampedValue.atMinimumTimestamp(6))
            .advanceProcessingTime(Duration.standardMinutes(10))
            .advanceWatermarkToInfinity();
    PCollection<Integer> streamVals = p.apply(new DirectTestStream<>(runner, testStream));

    TestClock clock = new TestClock();
    when(context.getClock()).thenReturn(clock);
    when(context.createRootBundle()).thenReturn(bundleFactory.createRootBundle());
    when(context.createBundle(streamVals))
        .thenReturn(bundleFactory.createBundle(streamVals), bundleFactory.createBundle(streamVals));

    AppliedPTransform<?, ?, ?> streamProducer = DirectGraphs.getProducer(streamVals);
    Collection<CommittedBundle<?>> initialInputs =
        new TestStreamEvaluatorFactory.InputProvider(context).getInitialInputs(streamProducer, 1);
    @SuppressWarnings("unchecked")
    CommittedBundle<TestStreamIndex<Integer>> initialBundle =
        (CommittedBundle<TestStreamIndex<Integer>>) Iterables.getOnlyElement(initialInputs);

    TransformEvaluator<TestStreamIndex<Integer>> firstEvaluator =
        factory.forApplication(streamProducer, initialBundle);
    firstEvaluator.processElement(Iterables.getOnlyElement(initialBundle.getElements()));
    TransformResult<TestStreamIndex<Integer>> firstResult = firstEvaluator.finishBundle();

    WindowedValue<TestStreamIndex<Integer>> firstResidual =
        (WindowedValue<TestStreamIndex<Integer>>)
            Iterables.getOnlyElement(firstResult.getUnprocessedElements());
    assertThat(firstResidual.getValue().getIndex(), equalTo(1));
    assertThat(firstResidual.getTimestamp(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    CommittedBundle<TestStreamIndex<Integer>> secondBundle =
        initialBundle.withElements(Collections.singleton(firstResidual));
    TransformEvaluator<TestStreamIndex<Integer>> secondEvaluator =
        factory.forApplication(streamProducer, secondBundle);
    secondEvaluator.processElement(firstResidual);
    TransformResult<TestStreamIndex<Integer>> secondResult = secondEvaluator.finishBundle();

    WindowedValue<TestStreamIndex<Integer>> secondResidual =
        (WindowedValue<TestStreamIndex<Integer>>)
            Iterables.getOnlyElement(secondResult.getUnprocessedElements());
    assertThat(secondResidual.getValue().getIndex(), equalTo(2));
    assertThat(secondResidual.getTimestamp(), equalTo(new Instant(0)));

    CommittedBundle<TestStreamIndex<Integer>> thirdBundle =
        secondBundle.withElements(Collections.singleton(secondResidual));
    TransformEvaluator<TestStreamIndex<Integer>> thirdEvaluator =
        factory.forApplication(streamProducer, thirdBundle);
    thirdEvaluator.processElement(secondResidual);
    TransformResult<TestStreamIndex<Integer>> thirdResult = thirdEvaluator.finishBundle();

    WindowedValue<TestStreamIndex<Integer>> thirdResidual =
        (WindowedValue<TestStreamIndex<Integer>>)
            Iterables.getOnlyElement(thirdResult.getUnprocessedElements());
    assertThat(thirdResidual.getValue().getIndex(), equalTo(3));
    assertThat(thirdResidual.getTimestamp(), equalTo(new Instant(0)));

    Instant start = clock.now();
    CommittedBundle<TestStreamIndex<Integer>> fourthBundle =
        thirdBundle.withElements(Collections.singleton(thirdResidual));
    TransformEvaluator<TestStreamIndex<Integer>> fourthEvaluator =
        factory.forApplication(streamProducer, fourthBundle);
    fourthEvaluator.processElement(thirdResidual);
    TransformResult<TestStreamIndex<Integer>> fourthResult = fourthEvaluator.finishBundle();

    assertThat(clock.now(), equalTo(start.plus(Duration.standardMinutes(10))));
    WindowedValue<TestStreamIndex<Integer>> fourthResidual =
        (WindowedValue<TestStreamIndex<Integer>>)
            Iterables.getOnlyElement(fourthResult.getUnprocessedElements());
    assertThat(fourthResidual.getValue().getIndex(), equalTo(4));
    assertThat(fourthResidual.getTimestamp(), equalTo(new Instant(0)));

    CommittedBundle<TestStreamIndex<Integer>> fifthBundle =
        thirdBundle.withElements(Collections.singleton(fourthResidual));
    TransformEvaluator<TestStreamIndex<Integer>> fifthEvaluator =
        factory.forApplication(streamProducer, fifthBundle);
    fifthEvaluator.processElement(fourthResidual);
    TransformResult<TestStreamIndex<Integer>> fifthResult = fifthEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(1),
            WindowedValue.valueInGlobalWindow(2),
            WindowedValue.valueInGlobalWindow(3)));

    assertThat(
        Iterables.getOnlyElement(thirdResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(4),
            WindowedValue.valueInGlobalWindow(5),
            WindowedValue.valueInGlobalWindow(6)));

    assertThat(fifthResult.getOutputBundles(), Matchers.emptyIterable());
    assertThat(fifthResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(fifthResult.getUnprocessedElements(), Matchers.emptyIterable());
  }
}
