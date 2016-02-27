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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.io.CountingSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
/**
 * Tests for {@link UnboundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadEvaluatorFactoryTest {
  private PCollection<Long> longs;
  private TransformEvaluatorFactory factory;
  private InProcessEvaluationContext context;
  private UncommittedBundle<Long> output;

  @Before
  public void setup() {
    UnboundedSource<Long, ?> source =
        CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    longs = p.apply(Read.from(source));

    factory = new UnboundedReadEvaluatorFactory();
    context = mock(InProcessEvaluationContext.class);
    output = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(output);
  }

  @Test
  public void unboundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));
  }

  /**
   * Demonstrate that multiple sequential creations will produce additional elements if the source
   * can provide them.
   */
  @Test
  public void unboundedSourceInMemoryTransformEvaluatorMultipleSequentialCalls() throws Exception {
    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));

    UncommittedBundle<Long> secondOutput = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();
    assertThat(
        secondResult.getWatermarkHold(),
        Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        secondOutput.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(11L), tgw(12L), tgw(14L), tgw(18L), tgw(19L), tgw(17L), tgw(16L),
            tgw(15L), tgw(13L), tgw(10L)));
  }

  // TODO: Once the source is split into multiple sources before evaluating, this test will have to
  // be updated.
  /**
   * Demonstrate that only a single unfinished instance of TransformEvaluator can be created at a
   * time, with other calls returning an empty evaluator.
   */
  @Test
  public void unboundedSourceWithMultipleSimultaneousEvaluatorsIndependent() throws Exception {
    UncommittedBundle<Long> secondOutput = InProcessBundle.unkeyed(longs);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);

    InProcessTransformResult secondResult = secondEvaluator.finishBundle();
    InProcessTransformResult result = evaluator.finishBundle();

    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(
            tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L), tgw(3L),
            tgw(0L)));

    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    assertThat(secondOutput.commit(Instant.now()).getElements(), emptyIterable());
  }

  /**
   * A terse alias for producing timestamped longs in the {@link GlobalWindow}, where
   * the timestamp is the epoch offset by the value of the element.
   */
  private static WindowedValue<Long> tgw(Long elem) {
    return WindowedValue.timestampedValueInGlobalWindow(elem, new Instant(elem));
  }

  private static class LongToInstantFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }
}
