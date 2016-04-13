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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ViewEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class ViewEvaluatorFactoryTest {
  private BundleFactory bundleFactory = InProcessBundleFactory.create();

  @Test
  public void testInMemoryEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bar"));
    CreatePCollectionView<String, Iterable<String>> createView =
        CreatePCollectionView.of(
            PCollectionViews.iterableView(p, input.getWindowingStrategy(), StringUtf8Coder.of()));
    PCollection<Iterable<String>> concat =
        input.apply(WithKeys.<Void, String>of((Void) null))
            .setCoder(KvCoder.of(VoidCoder.of(), StringUtf8Coder.of()))
            .apply(GroupByKey.<Void, String>create())
            .apply(Values.<Iterable<String>>create());
    PCollectionView<Iterable<String>> view =
        concat.apply(new ViewEvaluatorFactory.WriteView<>(createView));

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    TestViewWriter<String, Iterable<String>> viewWriter = new TestViewWriter<>();
    when(context.createPCollectionViewWriter(concat, view)).thenReturn(viewWriter);

    CommittedBundle<String> inputBundle =
        bundleFactory.createRootBundle(input).commit(Instant.now());
    TransformEvaluator<Iterable<String>> evaluator =
        new ViewEvaluatorFactory()
            .forApplication(view.getProducingTransformInternal(), inputBundle, context);

    evaluator.processElement(
        WindowedValue.<Iterable<String>>valueInGlobalWindow(ImmutableList.of("foo", "bar")));
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
