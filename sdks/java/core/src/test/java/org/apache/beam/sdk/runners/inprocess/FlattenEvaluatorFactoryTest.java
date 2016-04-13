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
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlattenEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class FlattenEvaluatorFactoryTest {
  private BundleFactory bundleFactory = InProcessBundleFactory.create();
  @Test
  public void testFlattenInMemoryEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> left = p.apply("left", Create.of(1, 2, 4));
    PCollection<Integer> right = p.apply("right", Create.of(-1, 2, -4));
    PCollectionList<Integer> list = PCollectionList.of(left).and(right);

    PCollection<Integer> flattened = list.apply(Flatten.<Integer>pCollections());

    CommittedBundle<Integer> leftBundle =
        bundleFactory.createRootBundle(left).commit(Instant.now());
    CommittedBundle<Integer> rightBundle =
        bundleFactory.createRootBundle(right).commit(Instant.now());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);

    UncommittedBundle<Integer> flattenedLeftBundle = bundleFactory.createRootBundle(flattened);
    UncommittedBundle<Integer> flattenedRightBundle = bundleFactory.createRootBundle(flattened);

    when(context.createBundle(leftBundle, flattened)).thenReturn(flattenedLeftBundle);
    when(context.createBundle(rightBundle, flattened)).thenReturn(flattenedRightBundle);

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory();
    TransformEvaluator<Integer> leftSideEvaluator =
        factory.forApplication(flattened.getProducingTransformInternal(), leftBundle, context);
    TransformEvaluator<Integer> rightSideEvaluator =
        factory.forApplication(
            flattened.getProducingTransformInternal(),
            rightBundle,
            context);

    leftSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    rightSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(-1));
    leftSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)));
    leftSideEvaluator.processElement(WindowedValue.valueInEmptyWindows(4, PaneInfo.NO_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.valueInEmptyWindows(2, PaneInfo.ON_TIME_AND_ONLY_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)));

    InProcessTransformResult rightSideResult = rightSideEvaluator.finishBundle();
    InProcessTransformResult leftSideResult = leftSideEvaluator.finishBundle();

    assertThat(
        rightSideResult.getOutputBundles(),
        Matchers.<UncommittedBundle<?>>contains(flattenedRightBundle));
    assertThat(
        rightSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattened.getProducingTransformInternal()));
    assertThat(
        leftSideResult.getOutputBundles(),
        Matchers.<UncommittedBundle<?>>contains(flattenedLeftBundle));
    assertThat(
        leftSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattened.getProducingTransformInternal()));

    assertThat(
        flattenedLeftBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)),
            WindowedValue.valueInEmptyWindows(4, PaneInfo.NO_FIRING),
            WindowedValue.valueInGlobalWindow(1)));
    assertThat(
        flattenedRightBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.valueInEmptyWindows(2, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)),
            WindowedValue.valueInGlobalWindow(-1)));
  }

  @Test
  public void testFlattenInMemoryEvaluatorWithEmptyPCollectionList() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollectionList<Integer> list = PCollectionList.empty(p);

    PCollection<Integer> flattened = list.apply(Flatten.<Integer>pCollections());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory();
    TransformEvaluator<Integer> emptyEvaluator =
        factory.forApplication(flattened.getProducingTransformInternal(), null, context);

    InProcessTransformResult leftSideResult = emptyEvaluator.finishBundle();

    assertThat(leftSideResult.getOutputBundles(), emptyIterable());
    assertThat(
        leftSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattened.getProducingTransformInternal()));
  }

}
