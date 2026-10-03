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
package org.apache.beam.sdk;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CompositePipeline}. */
@RunWith(JUnit4.class)
public class CompositePipelineTest {

  private static final PipelineOptions OPTIONS = PipelineOptionsFactory.create();

  /** A pipeline whose root is named {@code rootName} (or unnamed if empty). */
  private static Pipeline pipelineWithRoot(String rootName) {
    TransformHierarchy hierarchy = new TransformHierarchy(ResourceHints.create());
    if (!rootName.isEmpty()) {
      hierarchy.setRootName(rootName);
    }
    return Pipeline.forTransformHierarchy(hierarchy, PipelineOptionsFactory.create());
  }

  /** Applies {@code Create -> Map} with fixed names and a {@link PAssert} to {@code pipeline}. */
  private static PCollection<Integer> applyCreateAndMap(Pipeline pipeline, Integer... values) {
    PCollection<Integer> out =
        pipeline
            .apply("Create", Create.of(ImmutableList.copyOf(values)))
            .apply("Map", MapElements.into(TypeDescriptors.integers()).via(x -> x + 1));
    PAssert.that(out).empty(); // never run; only counted
    return out;
  }

  /** Full names of every non-root transform reached by traversing {@code pipeline}. */
  private static List<String> fullNames(Pipeline pipeline) {
    List<String> names = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              names.add(node.getFullName());
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            names.add(node.getFullName());
          }
        });
    return names;
  }

  @Test
  public void testTraversalCoversEveryMemberWithScopedNames() {
    Pipeline a = pipelineWithRoot("a");
    Pipeline b = pipelineWithRoot("b");
    PCollection<Integer> outA = applyCreateAndMap(a, 1, 2);
    PCollection<Integer> outB = applyCreateAndMap(b, 3);

    CompositePipeline composite = new CompositePipeline(OPTIONS, ImmutableList.of(a, b));
    assertThat(composite.getMemberPipelines(), contains(a, b));

    List<String> names = fullNames(composite);
    assertThat(names, hasItems("a/Create", "a/Map", "b/Create", "b/Map"));
    assertThat(names, not(hasItem("Create")));
    assertEquals(2, PAssert.countAsserts(composite));
    // PCollection names follow their producing transform's full name, so they are scoped too.
    assertThat(outA.getName(), startsWith("a/"));
    assertThat(outB.getName(), startsWith("b/"));
  }

  @Test
  public void testTranslatesToProtoWithDisjointUniqueNames() {
    Pipeline a = pipelineWithRoot("a");
    Pipeline b = pipelineWithRoot("b");
    applyCreateAndMap(a, 1);
    applyCreateAndMap(b, 2);

    RunnerApi.Pipeline proto =
        PipelineTranslation.toProto(new CompositePipeline(OPTIONS, ImmutableList.of(a, b)));

    List<String> uniqueNames = new ArrayList<>();
    for (RunnerApi.PTransform transform : proto.getComponents().getTransformsMap().values()) {
      uniqueNames.add(transform.getUniqueName());
    }
    assertThat(uniqueNames, hasItems("a/Create", "a/Map", "b/Create", "b/Map"));
    // Both members' top-level transforms are roots of the merged graph.
    List<String> rootNames = new ArrayList<>();
    for (String id : proto.getRootTransformIdsList()) {
      rootNames.add(proto.getComponents().getTransformsOrThrow(id).getUniqueName());
    }
    assertThat(rootNames, hasItems("a/Create", "a/Map", "b/Create", "b/Map"));
    assertThat(rootNames, everyItem(not(startsWith("/"))));
  }

  @Test
  public void testReplaceAllReachesEveryMember() {
    Pipeline a = pipelineWithRoot("a");
    Pipeline b = pipelineWithRoot("b");
    a.apply("Seq", GenerateSequence.from(0).to(10));
    b.apply("Seq", GenerateSequence.from(0).to(10));
    CompositePipeline composite = new CompositePipeline(OPTIONS, ImmutableList.of(a, b));

    composite.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                application -> application.getTransform() instanceof GenerateSequence,
                new PipelineTest.GenerateSequenceToCreateOverride())));

    Map<String, PTransform<?, ?>> topLevel = new HashMap<>();
    composite.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode() && node.getEnclosingNode().isRootNode()) {
              topLevel.put(node.getFullName(), node.getTransform());
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    assertThat(topLevel.get("a/Seq"), instanceOf(Create.Values.class));
    assertThat(topLevel.get("b/Seq"), instanceOf(Create.Values.class));
    // Members were replaced in place, so a member on its own sees the replacement too.
    assertThat(fullNames(a), everyItem(startsWith("a/")));
  }

  @Test
  public void testRejectsMembersWithCollidingTopLevelNames() {
    Pipeline unnamed0 = pipelineWithRoot("");
    Pipeline unnamed1 = pipelineWithRoot("");
    applyCreateAndMap(unnamed0, 1);
    applyCreateAndMap(unnamed1, 2);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new CompositePipeline(OPTIONS, ImmutableList.of(unnamed0, unnamed1)));
    assertThat(e.getMessage(), containsString("\"Create\""));
    assertThat(e.getMessage(), containsString("setRootName"));

    Pipeline sameRoot0 = pipelineWithRoot("same");
    Pipeline sameRoot1 = pipelineWithRoot("same");
    applyCreateAndMap(sameRoot0, 1);
    applyCreateAndMap(sameRoot1, 2);
    assertThrows(
        IllegalArgumentException.class,
        () -> new CompositePipeline(OPTIONS, ImmutableList.of(sameRoot0, sameRoot1)));
  }

  @Test
  public void testAcceptsMembersWithDistinctNamesAndNoRoot() {
    // Root names are the easy way to get disjoint names, but any disjoint naming is accepted.
    Pipeline p0 = pipelineWithRoot("");
    Pipeline p1 = pipelineWithRoot("");
    p0.apply("First", Create.of(1));
    p1.apply("Second", Create.of(2));
    assertThat(
        fullNames(new CompositePipeline(OPTIONS, ImmutableList.of(p0, p1))),
        hasItems("First", "Second"));
  }

  @Test
  public void testApplyIsUnsupported() {
    Pipeline a = pipelineWithRoot("a");
    applyCreateAndMap(a, 1);
    CompositePipeline composite = new CompositePipeline(OPTIONS, ImmutableList.of(a));
    assertThrows(UnsupportedOperationException.class, () -> composite.apply(Create.of(1)));
    assertThrows(UnsupportedOperationException.class, () -> composite.apply("Named", Create.of(1)));
  }
}
