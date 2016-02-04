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
package com.google.cloud.dataflow.sdk.runners.inprocess.evaluator;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.CountingSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluatorFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BoundedReadEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class BoundedReadEvaluatorFactoryTest {
  @Test
  public void boundedSourceInMemoryTransformEvaluatorProducesElements() throws Exception {
    BoundedSource<Long> source = CountingSource.upTo(10L);
    TestPipeline p = TestPipeline.create();
    PCollection<Long> longs = p.apply(Read.from(source));

    TransformEvaluatorFactory factory = new BoundedReadEvaluatorFactory();
    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    Bundle<Long> output = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(
        output.getElements(),
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  @Test
  public void boundedSourceInMemoryTransformEvaluatorMultipleCalls() throws Exception {
    BoundedSource<Long> source = CountingSource.upTo(10L);
    TestPipeline p = TestPipeline.create();
    PCollection<Long> longs = p.apply(Read.from(source));

    TransformEvaluatorFactory factory = new BoundedReadEvaluatorFactory();
    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    Bundle<Long> output = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(output);

    TransformEvaluator<?> evaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(
        output.getElements(),
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));

    Bundle<Long> secondOutput = InProcessBundle.unkeyed(longs);
    when(context.createRootBundle(longs)).thenReturn(secondOutput);
    TransformEvaluator<?> secondEvaluator =
        factory.forApplication(longs.getProducingTransformInternal(), null, context);
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(secondOutput.getElements(), emptyIterable());
    assertThat(
        output.getElements(),
        containsInAnyOrder(
            gw(1L), gw(2L), gw(4L), gw(8L), gw(9L), gw(7L), gw(6L), gw(5L), gw(3L), gw(0L)));
  }

  private static WindowedValue<Long> gw(Long elem) {
    return WindowedValue.valueInGlobalWindow(elem);
  }
}

