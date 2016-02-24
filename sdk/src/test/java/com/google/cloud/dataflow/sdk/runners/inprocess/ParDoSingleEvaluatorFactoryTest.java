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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessExecutionContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link ParDoSingleEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class ParDoSingleEvaluatorFactoryTest implements Serializable {
  @Test
  public void testParDoInMemoryTransformEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));
    PCollection<Integer> collection = input.apply(ParDo.of(new DoFn<String, Integer>() {
      @Override public void processElement(ProcessContext c) {
        c.output(c.element().length());
      }
    }));
    CommittedBundle<String> inputBundle = InProcessBundle.unkeyed(input).commit(Instant.now());

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<Integer> outputBundle =
        InProcessBundle.unkeyed(collection);
    when(evaluationContext.createBundle(inputBundle, collection)).thenReturn(outputBundle);
    InProcessExecutionContext executionContext = new InProcessExecutionContext();
    when(evaluationContext.getExecutionContext(collection.getProducingTransformInternal()))
        .thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator<String> evaluator =
        new ParDoSingleEvaluatorFactory().forApplication(
            collection.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(result.getOutputBundles(), Matchers.<UncommittedBundle<?>>contains(outputBundle));
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(result.getCounters(), equalTo(counters));

    assertThat(
        outputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<Integer>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(3),
            WindowedValue.timestampedValueInGlobalWindow(4, new Instant(1000)),
            WindowedValue.valueInGlobalWindow(5, PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void testSideOutputToUndeclaredSideOutputSucceeds() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));
    final TupleTag<Integer> sideOutputTag = new TupleTag<Integer>() {};
    PCollection<Integer> collection = input.apply(ParDo.of(new DoFn<String, Integer>() {
      @Override public void processElement(ProcessContext c) {
        c.sideOutput(sideOutputTag, c.element().length());
      }
    }));
    CommittedBundle<String> inputBundle = InProcessBundle.unkeyed(input).commit(Instant.now());

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<Integer> outputBundle =
        InProcessBundle.unkeyed(collection);
    when(evaluationContext.createBundle(inputBundle, collection)).thenReturn(outputBundle);
    InProcessExecutionContext executionContext =
        new InProcessExecutionContext();
    when(evaluationContext.getExecutionContext(collection.getProducingTransformInternal()))
        .thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    TransformEvaluator<String> evaluator =
        new ParDoSingleEvaluatorFactory().forApplication(
            collection.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getOutputBundles(), Matchers.<UncommittedBundle<?>>containsInAnyOrder(outputBundle));
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(result.getCounters(), equalTo(counters));
  }
}

