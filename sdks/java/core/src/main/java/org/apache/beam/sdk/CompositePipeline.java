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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A read-only {@link Pipeline} that presents several independently constructed member pipelines
 * as one graph, so that a runner can translate and execute them together as a single job.
 *
 * <p>Only the two operations runners use to translate a pipeline are composed: {@link
 * #traverseTopologically} visits every member in turn, and {@link #replaceAll} applies the runner's
 * overrides to every member. Everything else is the composite's own: {@link #getOptions()} are the
 * options passed to the constructor (member options are ignored), the coder and schema registries
 * are fresh and empty, and transforms cannot be applied to it. Visitors that need a member's state
 * (for example its {@link #getCoderRegistry()}) should use {@link
 * TransformHierarchy.Node#getPipeline()} rather than the pipeline being traversed.
 *
 * <p>Members must have pairwise distinct top-level transform names (and therefore distinct full
 * names throughout, since every nested name and {@link org.apache.beam.sdk.values.PCollection} name
 * is derived from its enclosing transform's); this is checked at construction. The simplest way to
 * guarantee it is to give each member a unique root name via {@link TransformHierarchy#setRootName}
 * before applying any transform.
 */
@Internal
public final class CompositePipeline extends Pipeline {

  private final List<Pipeline> memberPipelines;

  /**
   * Creates a composite over {@code memberPipelines}.
   *
   * @throws IllegalArgumentException if two members share a top-level transform name
   */
  public CompositePipeline(PipelineOptions options, List<Pipeline> memberPipelines) {
    super(options);
    this.memberPipelines = ImmutableList.copyOf(memberPipelines);
    checkTopLevelNamesDisjoint(this.memberPipelines);
  }

  /** The member pipelines, in the order they were given and are traversed. */
  public List<Pipeline> getMemberPipelines() {
    return memberPipelines;
  }

  @Override
  public void traverseTopologically(PipelineVisitor visitor) {
    for (Pipeline member : memberPipelines) {
      member.traverseTopologically(visitor);
    }
  }

  @Override
  public void replaceAll(List<PTransformOverride> overrides) {
    for (Pipeline member : memberPipelines) {
      member.replaceAll(overrides);
    }
  }

  /**
   * @throws UnsupportedOperationException always; a composite is a read-only view.
   */
  @Override
  public <OutputT extends POutput> OutputT apply(PTransform<? super PBegin, OutputT> root) {
    throw new UnsupportedOperationException("Transforms cannot be applied to a CompositePipeline");
  }

  /**
   * @throws UnsupportedOperationException always; a composite is a read-only view.
   */
  @Override
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PBegin, OutputT> root) {
    throw new UnsupportedOperationException("Transforms cannot be applied to a CompositePipeline");
  }

  private static void checkTopLevelNamesDisjoint(List<Pipeline> members) {
    Map<String, Integer> owners = new HashMap<>();
    for (int i = 0; i < members.size(); i++) {
      for (String name : topLevelTransformNames(members.get(i))) {
        Integer previous = owners.putIfAbsent(name, i);
        checkArgument(
            previous == null,
            "Member pipelines %s and %s both have a top-level transform named \"%s\"; give each"
                + " member a unique root name (see TransformHierarchy#setRootName) so that their"
                + " transform names are disjoint",
            previous,
            i,
            name);
      }
    }
  }

  private static List<String> topLevelTransformNames(Pipeline pipeline) {
    List<String> names = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (isTopLevel(node)) {
              names.add(node.getFullName());
              return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (isTopLevel(node)) {
              names.add(node.getFullName());
            }
          }

          private boolean isTopLevel(TransformHierarchy.Node node) {
            TransformHierarchy.Node enclosing = node.getEnclosingNode();
            return enclosing != null && enclosing.isRootNode();
          }
        });
    return names;
  }
}
