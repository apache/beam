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
package org.apache.beam.sdk.transforms.display;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;

/**
 * Test utilities to evaluate the {@link DisplayData} in the context of a {@link PipelineRunner}.
 */
public class DisplayDataEvaluator {
  private final PipelineOptions options;

  /**
   * Create a new {@link DisplayDataEvaluator} using {@link TestPipeline#testingPipelineOptions()}.
   */
  public static DisplayDataEvaluator create() {
    return create(TestPipeline.testingPipelineOptions());
  }

  /**
   * Create a new {@link DisplayDataEvaluator} using the specified {@link PipelineOptions}.
   */
  public static DisplayDataEvaluator create(PipelineOptions pipelineOptions) {
    return new DisplayDataEvaluator(pipelineOptions);
  }

  private DisplayDataEvaluator(PipelineOptions options) {
    this.options = options;
  }

  /**
   * Traverse the specified {@link PTransform}, collecting {@link DisplayData} registered on the
   * inner primitive {@link PTransform PTransforms}.
   *
   * @return the set of {@link DisplayData} for primitive {@link PTransform PTransforms}.
   */
  public <InputT> Set<DisplayData> displayDataForPrimitiveTransforms(
      final PTransform<? super PCollection<InputT>, ? extends POutput> root) {
    return displayDataForPrimitiveTransforms(root, null);
  }

  /**
   * Traverse the specified {@link PTransform}, collecting {@link DisplayData} registered on the
   * inner primitive {@link PTransform PTransforms}.
   *
   * @param root The root {@link PTransform} to traverse
   * @param inputCoder The coder to set for the {@link PTransform} input, or null to infer the
   *                   default coder.
   *
   * @return the set of {@link DisplayData} for primitive {@link PTransform PTransforms}.
   */
  public <InputT> Set<DisplayData> displayDataForPrimitiveTransforms(
    final PTransform<? super PCollection<InputT>, ? extends POutput> root,
    Coder<InputT> inputCoder) {

    Create.Values<InputT> input = Create.of();
    if (inputCoder != null) {
      input = input.withCoder(inputCoder);
    }

    Pipeline pipeline = Pipeline.create(options);
    pipeline
      .apply(input)
      .apply(root);

    PrimitiveDisplayDataPTransformVisitor visitor = new PrimitiveDisplayDataPTransformVisitor(root);
    pipeline.traverseTopologically(visitor);
    return visitor.getPrimitivesDisplayData();
  }

  /**
   * Visits {@link PTransform PTransforms} in a pipeline, and collects {@link DisplayData} for
   * each primitive transform under a given composite root.
   */
  private static class PrimitiveDisplayDataPTransformVisitor
  extends Pipeline.PipelineVisitor.Defaults {
    private final PTransform root;
    private final Set<DisplayData> displayData;
    private boolean inCompositeRoot = false;

    PrimitiveDisplayDataPTransformVisitor(PTransform root) {
      this.root = root;
      this.displayData = Sets.newHashSet();
    }

    Set<DisplayData> getPrimitivesDisplayData() {
      return displayData;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
      if (Objects.equals(root, node.getTransform())) {
        inCompositeRoot = true;
      }

      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
      if (Objects.equals(root, node.getTransform())) {
        inCompositeRoot = false;
      }
    }

    @Override
    public void visitPrimitiveTransform(TransformTreeNode node) {
      if (inCompositeRoot || Objects.equals(root, node.getTransform())) {
        displayData.add(DisplayData.from(node.getTransform()));
      }
    }
  }
}
