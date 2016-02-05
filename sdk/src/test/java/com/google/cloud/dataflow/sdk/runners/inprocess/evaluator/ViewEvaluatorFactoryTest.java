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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import com.google.cloud.dataflow.sdk.runners.inprocess.TransformEvaluator;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ViewEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class ViewEvaluatorFactoryTest {
  @Test
  public void testInMemoryEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<String> input = p.apply(Create.of("foo", "bar"));
    PCollectionView<Iterable<String>> view = input.apply(View.<String>asIterable());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    TestViewWriter<String, Iterable<String>> viewWriter = new TestViewWriter<>();
    when(context.createPCollectionViewWriter(input, view)).thenReturn(viewWriter);

    CommittedBundle<String> inputBundle = InProcessBundle.unkeyed(input).commit(Instant.now());
    TransformEvaluator<String> evaluator = new ViewEvaluatorFactory().forApplication(
        view.getProducingTransformInternal(), inputBundle, context);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(WindowedValue.valueInGlobalWindow("bar"));
    assertThat(viewWriter.latest, nullValue());

    evaluator.finishBundle();
    assertThat(
        viewWriter.latest,
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("foo"), WindowedValue.valueInGlobalWindow("bar")));
  }

  private static class TestViewWriter<ElemT, ViewT> implements PCollectionViewWriter<ElemT, ViewT> {
    private Iterable<WindowedValue<ElemT>> latest;

    @Override
    public void add(Iterable<WindowedValue<ElemT>> values) {
      latest = values;
    }
  }
}

