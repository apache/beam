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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.io.CountingSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
/**
 * Tests for {@link UnboundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadEvaluatorFactoryTest {
  @Test
  public void unboundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    UnboundedSource<Long, ?> source =
        CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    PCollection<Long> longs = p.apply(Read.from(source));

    TransformEvaluatorFactory factory = new UnboundedReadEvaluatorFactory();
    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    UncommittedBundle<Long> output = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L),
            tgw(3L), tgw(0L)));
  }

  @Test
  public void unboundedSourceInMemoryTransformEvaluatorMultipleCalls() throws Exception {
    UnboundedSource<Long, ?> source =
        CountingSource.unboundedWithTimestampFn(new LongToInstantFn());
    TestPipeline p = TestPipeline.create();
    PCollection<Long> longs = p.apply(Read.from(source));

    TransformEvaluatorFactory factory = new UnboundedReadEvaluatorFactory();
    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    UncommittedBundle<Long> output = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getWatermarkHold(), Matchers.<ReadableInstant>lessThan(DateTime.now().toInstant()));
    assertThat(
        output.commit(Instant.now()).getElements(),
        containsInAnyOrder(tgw(1L), tgw(2L), tgw(4L), tgw(8L), tgw(9L), tgw(7L), tgw(6L), tgw(5L),
            tgw(3L), tgw(0L)));

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
