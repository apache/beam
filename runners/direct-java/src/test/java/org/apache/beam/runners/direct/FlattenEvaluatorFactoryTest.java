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
import static org.hamcrest.Matchers.emptyIterable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlattenEvaluatorFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({"keyfor"})
public class FlattenEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testFlattenInMemoryEvaluator() throws Exception {
    PCollection<Integer> left = p.apply("left", Create.of(1, 2, 4));
    PCollection<Integer> right = p.apply("right", Create.of(-1, 2, -4));
    PCollectionList<Integer> list = PCollectionList.of(left).and(right);

    PCollection<Integer> flattened = list.apply(Flatten.pCollections());

    CommittedBundle<Integer> leftBundle = bundleFactory.createBundle(left).commit(Instant.now());
    CommittedBundle<Integer> rightBundle = bundleFactory.createBundle(right).commit(Instant.now());

    EvaluationContext context = mock(EvaluationContext.class);

    UncommittedBundle<Integer> flattenedLeftBundle = bundleFactory.createBundle(flattened);
    UncommittedBundle<Integer> flattenedRightBundle = bundleFactory.createBundle(flattened);

    when(context.createBundle(flattened)).thenReturn(flattenedLeftBundle, flattenedRightBundle);

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory(context);
    AppliedPTransform<?, ?, ?> flattenedProducer = DirectGraphs.getProducer(flattened);
    TransformEvaluator<Integer> leftSideEvaluator =
        factory.forApplication(flattenedProducer, leftBundle);
    TransformEvaluator<Integer> rightSideEvaluator =
        factory.forApplication(flattenedProducer, rightBundle);

    leftSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    rightSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(-1));
    leftSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)));
    leftSideEvaluator.processElement(WindowedValue.valueInGlobalWindow(4, PaneInfo.NO_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.valueInGlobalWindow(2, PaneInfo.ON_TIME_AND_ONLY_FIRING));
    rightSideEvaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)));

    TransformResult<Integer> rightSideResult = rightSideEvaluator.finishBundle();
    TransformResult<Integer> leftSideResult = leftSideEvaluator.finishBundle();

    assertThat(rightSideResult.getOutputBundles(), Matchers.contains(flattenedRightBundle));
    assertThat(
        rightSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattenedProducer));
    assertThat(leftSideResult.getOutputBundles(), Matchers.contains(flattenedLeftBundle));
    assertThat(
        leftSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattenedProducer));

    assertThat(
        flattenedLeftBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1024)),
            WindowedValue.valueInGlobalWindow(4, PaneInfo.NO_FIRING),
            WindowedValue.valueInGlobalWindow(1)));
    assertThat(
        flattenedRightBundle.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(2, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.timestampedValueInGlobalWindow(-4, new Instant(-4096)),
            WindowedValue.valueInGlobalWindow(-1)));
  }

  @Test
  public void testFlattenInMemoryEvaluatorWithEmptyPCollectionList() throws Exception {
    PCollectionList<Integer> list = PCollectionList.empty(p);

    PCollection<Integer> flattened = list.apply(Flatten.pCollections());
    flattened.setCoder(VarIntCoder.of());

    EvaluationContext evaluationContext = mock(EvaluationContext.class);
    when(evaluationContext.createBundle(flattened))
        .thenReturn(bundleFactory.createBundle(flattened));

    FlattenEvaluatorFactory factory = new FlattenEvaluatorFactory(evaluationContext);
    AppliedPTransform<?, ?, ?> flattendProducer = DirectGraphs.getProducer(flattened);
    TransformEvaluator<Integer> emptyEvaluator =
        factory.forApplication(
            flattendProducer,
            bundleFactory.createRootBundle().commit(BoundedWindow.TIMESTAMP_MAX_VALUE));

    TransformResult<Integer> leftSideResult = emptyEvaluator.finishBundle();

    CommittedBundle<?> outputBundle =
        Iterables.getOnlyElement(leftSideResult.getOutputBundles()).commit(Instant.now());
    assertThat(outputBundle.getElements(), emptyIterable());
    assertThat(
        leftSideResult.getTransform(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(flattendProducer));
  }
}
