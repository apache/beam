/*
 * Copyright (C) 2016 Google Inc.
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

import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.io.CountingInput;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.PValue;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.List;

/**
 * Tests for {@link ConsumerTrackingPipelineVisitor}.
 */
@RunWith(JUnit4.class)
public class ConsumerTrackingPipelineVisitorTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private transient TestPipeline p = TestPipeline.create();
  private transient ConsumerTrackingPipelineVisitor visitor = new ConsumerTrackingPipelineVisitor();

  @Test
  public void getViewsReturnsViews() {
    PCollectionView<List<String>> listView =
        p.apply("listCreate", Create.of("foo", "bar"))
            .apply(
                ParDo.of(
                    new DoFn<String, String>() {
                      @Override
                      public void processElement(DoFn<String, String>.ProcessContext c)
                          throws Exception {
                        c.output(Integer.toString(c.element().length()));
                      }
                    }))
            .apply(View.<String>asList());
    PCollectionView<Object> singletonView =
        p.apply("singletonCreate", Create.<Object>of(1, 2, 3)).apply(View.<Object>asSingleton());
    p.traverseTopologically(visitor);
    assertThat(
        visitor.getViews(),
        Matchers.<PCollectionView<?>>containsInAnyOrder(listView, singletonView));
  }

  @Test
  public void getRootTransformsContainsPBegins() {
    PCollection<String> created = p.apply(Create.of("foo", "bar"));
    PCollection<Long> counted = p.apply(CountingInput.upTo(1234L));
    PCollection<Long> unCounted = p.apply(CountingInput.unbounded());
    p.traverseTopologically(visitor);
    assertThat(
        visitor.getRootTransforms(),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            created.getProducingTransformInternal(),
            counted.getProducingTransformInternal(),
            unCounted.getProducingTransformInternal()));
  }

  @Test
  public void getRootTransformsContainsEmptyFlatten() {
    PCollection<String> empty =
        PCollectionList.<String>empty(p).apply(Flatten.<String>pCollections());
    p.traverseTopologically(visitor);
    assertThat(
        visitor.getRootTransforms(),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            empty.getProducingTransformInternal()));
  }

  @Test
  public void getValueToConsumersSucceeds() {
    PCollection<String> created = p.apply(Create.of("1", "2", "3"));
    PCollection<String> transformed =
        created.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @Override
                  public void processElement(DoFn<String, String>.ProcessContext c)
                      throws Exception {
                    c.output(Integer.toString(c.element().length()));
                  }
                }));

    PCollection<String> flattened =
        PCollectionList.of(created).and(transformed).apply(Flatten.<String>pCollections());

    p.traverseTopologically(visitor);

    assertThat(
        visitor.getValueToConsumers().get(created),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            transformed.getProducingTransformInternal(),
            flattened.getProducingTransformInternal()));
    assertThat(
        visitor.getValueToConsumers().get(transformed),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            flattened.getProducingTransformInternal()));
    assertThat(visitor.getValueToConsumers().get(flattened), emptyIterable());
  }

  @Test
  public void getUnfinalizedPValuesContainsDanglingOutputs() {
    PCollection<String> created = p.apply(Create.of("1", "2", "3"));
    PCollection<String> transformed =
        created.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @Override
                  public void processElement(DoFn<String, String>.ProcessContext c)
                      throws Exception {
                    c.output(Integer.toString(c.element().length()));
                  }
                }));

    p.traverseTopologically(visitor);
    assertThat(visitor.getUnfinalizedPValues(), Matchers.<PValue>contains(transformed));
  }

  @Test
  public void getUnfinalizedPValuesEmpty() {
    p.apply(Create.of("1", "2", "3"))
        .apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @Override
                  public void processElement(DoFn<String, String>.ProcessContext c)
                      throws Exception {
                    c.output(Integer.toString(c.element().length()));
                  }
                }))
        .apply(
            new PTransform<PInput, PDone>() {
              @Override
              public PDone apply(PInput input) {
                return PDone.in(input.getPipeline());
              }
            });

    p.traverseTopologically(visitor);
    assertThat(visitor.getUnfinalizedPValues(), emptyIterable());
  }

  @Test
  public void traverseMultipleTimesThrows() {
    p.apply(Create.of(1, 2, 3));

    p.traverseTopologically(visitor);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(ConsumerTrackingPipelineVisitor.class.getSimpleName());
    thrown.expectMessage("is finalized");
    p.traverseTopologically(visitor);
  }

  @Test
  public void traverseIndependentPathsSucceeds() {
    p.apply("left", Create.of(1, 2, 3));
    p.apply("right", Create.of("foo", "bar", "baz"));

    p.traverseTopologically(visitor);
  }

  @Test
  public void getRootTransformsWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getRootTransforms");
    visitor.getRootTransforms();
  }
  @Test
  public void getStepNamesWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getStepNames");
    visitor.getStepNames();
  }
  @Test
  public void getUnfinalizedPValuesWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getUnfinalizedPValues");
    visitor.getUnfinalizedPValues();
  }

  @Test
  public void getValueToConsumersWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getValueToConsumers");
    visitor.getValueToConsumers();
  }

  @Test
  public void getViewsWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getViews");
    visitor.getViews();
  }
}
