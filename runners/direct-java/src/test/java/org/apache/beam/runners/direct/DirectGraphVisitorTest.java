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

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DirectGraphVisitor}.
 */
@RunWith(JUnit4.class)
public class DirectGraphVisitorTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient TestPipeline p = TestPipeline.create()
                                                      .enableAbandonedNodeEnforcement(false);

  private transient DirectGraphVisitor visitor = new DirectGraphVisitor();

  @Test
  public void getViewsReturnsViews() {
    PCollectionView<List<String>> listView =
        p.apply("listCreate", Create.of("foo", "bar"))
            .apply(
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
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
        visitor.getGraph().getViews(),
        Matchers.<PCollectionView<?>>containsInAnyOrder(listView, singletonView));
  }

  @Test
  public void getRootTransformsContainsRootTransforms() {
    PCollection<String> created = p.apply(Create.of("foo", "bar"));
    PCollection<Long> counted = p.apply(Read.from(CountingSource.upTo(1234L)));
    PCollection<Long> unCounted = p.apply(GenerateSequence.from(0));
    p.traverseTopologically(visitor);
    DirectGraph graph = visitor.getGraph();
    assertThat(graph.getRootTransforms(), hasSize(3));
    assertThat(
        graph.getRootTransforms(),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            graph.getProducer(created), graph.getProducer(counted), graph.getProducer(unCounted)));
    for (AppliedPTransform<?, ?, ?> root : graph.getRootTransforms())  {
      // Root transforms will have no inputs
      assertThat(root.getInputs().entrySet(), emptyIterable());
      assertThat(
          Iterables.getOnlyElement(root.getOutputs().values()),
          Matchers.<POutput>isOneOf(created, counted, unCounted));
    }
  }

  @Test
  public void getRootTransformsContainsEmptyFlatten() {
    PCollections<String> flatten = Flatten.pCollections();
    PCollectionList<String> emptyList = PCollectionList.empty(p);
    PCollection<String> empty = emptyList.apply(flatten);
    empty.setCoder(StringUtf8Coder.of());
    p.traverseTopologically(visitor);
    DirectGraph graph = visitor.getGraph();
    assertThat(
        graph.getRootTransforms(),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(graph.getProducer(empty)));
    AppliedPTransform<?, ?, ?> onlyRoot = Iterables.getOnlyElement(graph.getRootTransforms());
    assertThat(onlyRoot.getTransform(), Matchers.<PTransform<?, ?>>equalTo(flatten));
    assertThat(onlyRoot.getInputs().entrySet(), emptyIterable());
    assertThat(onlyRoot.getOutputs(), equalTo(empty.expand()));
  }

  @Test
  public void getValueToConsumersSucceeds() {
    PCollection<String> created = p.apply(Create.of("1", "2", "3"));
    PCollection<String> transformed =
        created.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(DoFn<String, String>.ProcessContext c)
                      throws Exception {
                    c.output(Integer.toString(c.element().length()));
                  }
                }));

    PCollection<String> flattened =
        PCollectionList.of(created).and(transformed).apply(Flatten.<String>pCollections());

    p.traverseTopologically(visitor);

    DirectGraph graph = visitor.getGraph();
    AppliedPTransform<?, ?, ?> transformedProducer =
        graph.getProducer(transformed);
    AppliedPTransform<?, ?, ?> flattenedProducer =
        graph.getProducer(flattened);

    assertThat(
        graph.getPrimitiveConsumers(created),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(
            transformedProducer, flattenedProducer));
    assertThat(
        graph.getPrimitiveConsumers(transformed),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(flattenedProducer));
    assertThat(graph.getPrimitiveConsumers(flattened), emptyIterable());
  }

  @Test
  public void getValueToConsumersWithDuplicateInputSucceeds() {
    PCollection<String> created = p.apply(Create.of("1", "2", "3"));

    PCollection<String> flattened =
        PCollectionList.of(created).and(created).apply(Flatten.<String>pCollections());

    p.traverseTopologically(visitor);

    DirectGraph graph = visitor.getGraph();
    AppliedPTransform<?, ?, ?> flattenedProducer = graph.getProducer(flattened);

    assertThat(
        graph.getPrimitiveConsumers(created),
        Matchers.<AppliedPTransform<?, ?, ?>>containsInAnyOrder(flattenedProducer,
            flattenedProducer));
    assertThat(graph.getPrimitiveConsumers(flattened), emptyIterable());
  }

  @Test
  public void getStepNamesContainsAllTransforms() {
    PCollection<String> created = p.apply(Create.of("1", "2", "3"));
    PCollection<String> transformed =
        created.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(DoFn<String, String>.ProcessContext c)
                      throws Exception {
                    c.output(Integer.toString(c.element().length()));
                  }
                }));
    PDone finished =
        transformed.apply(
            new PTransform<PInput, PDone>() {
              @Override
              public PDone expand(PInput input) {
                return PDone.in(input.getPipeline());
              }
            });

    p.traverseTopologically(visitor);
    DirectGraph graph = visitor.getGraph();
    assertThat(graph.getStepName(graph.getProducer(created)), equalTo("s0"));
    assertThat(graph.getStepName(graph.getProducer(transformed)), equalTo("s1"));
    // finished doesn't have a producer, because it's not a PValue.
    // TODO: Demonstrate that PCollectionList/Tuple and other composite PValues are either safe to
    // use, or make them so.
  }

  @Test
  public void traverseMultipleTimesThrows() {
    p.apply(Create.of(1, 2, 3));

    p.traverseTopologically(visitor);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(DirectGraphVisitor.class.getSimpleName());
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
  public void getGraphWithoutVisitingThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("get a graph");
    visitor.getGraph();
  }
}
