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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessExecutionContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessTransformResult;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.ParDo.BoundMulti;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link ParDoMultiEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class ParDoMultiEvaluatorFactoryTest implements Serializable {
  @Test
  public void testParDoMultiInMemoryTransformEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));

    TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<KV<String, Integer>>() {};
    final TupleTag<String> elementTag = new TupleTag<>();
    final TupleTag<Integer> lengthTag = new TupleTag<>();

    BoundMulti<String, KV<String, Integer>> pardo =
        ParDo.of(new DoFn<String, KV<String, Integer>>() {
          @Override
          public void processElement(ProcessContext c) {
            c.output(KV.<String, Integer>of(c.element(), c.element().length()));
            c.sideOutput(elementTag, c.element());
            c.sideOutput(lengthTag, c.element().length());
          }
        }).withOutputTags(mainOutputTag, TupleTagList.of(elementTag).and(lengthTag));
    PCollectionTuple outputTuple = input.apply(pardo);

    Bundle<String> inputBundle = InProcessBundle.unkeyed(input);

    PCollection<KV<String, Integer>> mainOutput = outputTuple.get(mainOutputTag);
    PCollection<String> elementOutput = outputTuple.get(elementTag);
    PCollection<Integer> lengthOutput = outputTuple.get(lengthTag);

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    Bundle<KV<String, Integer>> mainOutputBundle = InProcessBundle.unkeyed(mainOutput);
    Bundle<String> elementOutputBundle = InProcessBundle.unkeyed(elementOutput);
    Bundle<Integer> lengthOutputBundle = InProcessBundle.unkeyed(lengthOutput);

    when(evaluationContext.createBundle(inputBundle, mainOutput)).thenReturn(mainOutputBundle);
    when(evaluationContext.createBundle(inputBundle, elementOutput))
        .thenReturn(elementOutputBundle);
    when(evaluationContext.createBundle(inputBundle, lengthOutput)).thenReturn(lengthOutputBundle);

    InProcessExecutionContext executionContext = new InProcessExecutionContext();
    when(evaluationContext.getExecutionContext(mainOutput.getProducingTransformInternal()))
        .thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator<String> evaluator =
        new ParDoMultiEvaluatorFactory().forApplication(
            mainOutput.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getBundles(),
        Matchers.<Bundle<?>>containsInAnyOrder(
            lengthOutputBundle, mainOutputBundle, elementOutputBundle));
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(result.getCounters(), equalTo(counters));

    assertThat(
        mainOutputBundle.getElements(),
        Matchers.<WindowedValue<KV<String, Integer>>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("foo", 3)),
            WindowedValue.timestampedValueInGlobalWindow(KV.of("bara", 4), new Instant(1000)),
            WindowedValue.valueInGlobalWindow(
                KV.of("bazam", 5), PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(
        elementOutputBundle.getElements(),
        Matchers.<WindowedValue<String>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("foo"),
            WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)),
            WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(
        lengthOutputBundle.getElements(),
        Matchers.<WindowedValue<Integer>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(3),
            WindowedValue.timestampedValueInGlobalWindow(4, new Instant(1000)),
            WindowedValue.valueInGlobalWindow(5, PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }
}

