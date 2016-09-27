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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TestStreamEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class TestStreamEvaluatorFactoryTest {
  private TestStreamEvaluatorFactory factory;
  private BundleFactory bundleFactory;
  private EvaluationContext context;

  @Before
  public void setup() {
    context = mock(EvaluationContext.class);
    factory = new TestStreamEvaluatorFactory(context);
    bundleFactory = ImmutableListBundleFactory.create();
  }

  /** Demonstrates that returned evaluators produce elements in sequence. */
  @Test
  public void producesElementsInSequence() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> streamVals =
        p.apply(
            TestStream.create(VarIntCoder.of())
                .addElements(1, 2, 3)
                .addElements(4, 5, 6)
                .advanceWatermarkToInfinity());

    when(context.createBundle(streamVals))
        .thenReturn(bundleFactory.createBundle(streamVals), bundleFactory.createBundle(streamVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null);
    TransformResult firstResult = firstEvaluator.finishBundle();

    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null);
    TransformResult secondResult = secondEvaluator.finishBundle();

    TransformEvaluator<Object> thirdEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null);
    TransformResult thirdResult = thirdEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(1),
            WindowedValue.valueInGlobalWindow(2),
            WindowedValue.valueInGlobalWindow(3)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(4),
            WindowedValue.valueInGlobalWindow(5),
            WindowedValue.valueInGlobalWindow(6)));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(Iterables.isEmpty(thirdResult.getOutputBundles()), is(true));
    assertThat(thirdResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  /** Demonstrates that at most one evaluator for an application is available at a time. */
  @Test
  public void onlyOneEvaluatorAtATime() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> streamVals =
        p.apply(
            TestStream.create(VarIntCoder.of()).addElements(4, 5, 6).advanceWatermarkToInfinity());

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null);

    // create a second evaluator before the first is finished. The evaluator should not be available
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null);
    assertThat(secondEvaluator, is(nullValue()));
  }

  /**
   * Demonstrates that multiple applications of the same {@link TestStream} produce separate
   * evaluators.
   */
  @Test
  public void multipleApplicationsMultipleEvaluators() throws Exception {
    TestPipeline p = TestPipeline.create();
    TestStream<Integer> stream =
        TestStream.create(VarIntCoder.of()).addElements(2).advanceWatermarkToInfinity();
    PCollection<Integer> firstVals = p.apply("Stream One", stream);
    PCollection<Integer> secondVals = p.apply("Stream A", stream);

    when(context.createBundle(firstVals)).thenReturn(bundleFactory.createBundle(firstVals));
    when(context.createBundle(secondVals)).thenReturn(bundleFactory.createBundle(secondVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(firstVals.getProducingTransformInternal(), null);
    // The two evaluators can exist independently
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(secondVals.getProducingTransformInternal(), null);

    TransformResult firstResult = firstEvaluator.finishBundle();
    TransformResult secondResult = secondEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    // They both produce equal results, and don't interfere with each other
    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  /**
   * Demonstrates that multiple applications of different {@link TestStream} produce independent
   * evaluators.
   */
  @Test
  public void multipleStreamsMultipleEvaluators() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> firstVals =
        p.apply(
            "Stream One",
            TestStream.create(VarIntCoder.of()).addElements(2).advanceWatermarkToInfinity());
    PCollection<String> secondVals =
        p.apply(
            "Stream A",
            TestStream.create(StringUtf8Coder.of())
                .addElements("Two")
                .advanceWatermarkToInfinity());

    when(context.createBundle(firstVals)).thenReturn(bundleFactory.createBundle(firstVals));
    when(context.createBundle(secondVals)).thenReturn(bundleFactory.createBundle(secondVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(firstVals.getProducingTransformInternal(), null);
    // The two evaluators can exist independently
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(secondVals.getProducingTransformInternal(), null);

    TransformResult firstResult = firstEvaluator.finishBundle();
    TransformResult secondResult = secondEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow("Two")));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }
}
