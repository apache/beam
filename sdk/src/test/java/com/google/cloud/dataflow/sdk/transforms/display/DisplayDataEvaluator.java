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

package com.google.cloud.dataflow.sdk.transforms.display;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;

/**
 * Test utilities to evaluate the {@link DisplayData} in the context of a {@link PipelineRunner}.
 */
public class DisplayDataEvaluator {
  private final PipelineOptions options;

  /**
   * Create a new {@link DisplayDataEvaluator} using options returned from
   * {@link #getDefaultOptions()}.
   */
  public static DisplayDataEvaluator create() {
    return create(getDefaultOptions());
  }

  /**
   * Create a new {@link DisplayDataEvaluator} using the specified {@link PipelineOptions}.
   */
  public static DisplayDataEvaluator create(PipelineOptions pipelineOptions) {
    return new DisplayDataEvaluator(pipelineOptions);
  }

  /**
   * The default {@link PipelineOptions} which will be used by {@link #create()}.
   */
  public static PipelineOptions getDefaultOptions() {
    return TestPipeline.testingPipelineOptions();
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
      .apply("Input", input)
      .apply("Transform", root);

    PrimitiveDisplayDataPTransformVisitor visitor = new PrimitiveDisplayDataPTransformVisitor(root);
    pipeline.traverseTopologically(visitor);
    return visitor.getPrimitivesDisplayData();
  }

  /**
   * Traverse the specified source {@link PTransform}, collecting {@link DisplayData} registered
   * on the inner primitive {@link PTransform PTransforms}.
   *
   * @param root The source root {@link PTransform} to traverse
   * @return the set of {@link DisplayData} for primitive source {@link PTransform PTransforms}.
   */
  public Set<DisplayData> displayDataForPrimitiveSourceTransforms(
      final PTransform<? super PBegin, ? extends POutput> root) {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("SourceTransform", root);

    return displayDataForPipeline(pipeline, root);
  }

  private static Set<DisplayData> displayDataForPipeline(Pipeline pipeline, PTransform root) {
    PrimitiveDisplayDataPTransformVisitor visitor = new PrimitiveDisplayDataPTransformVisitor(root);
    pipeline.traverseTopologically(visitor);
    return visitor.getPrimitivesDisplayData();
  }

  /**
   * Visits {@link PTransform PTransforms} in a pipeline, and collects {@link DisplayData} for
   * each primitive transform under a given composite root.
   */
  private static class PrimitiveDisplayDataPTransformVisitor implements Pipeline.PipelineVisitor {
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
    public void enterCompositeTransform(TransformTreeNode node) {
      if (Objects.equals(root, node.getTransform())) {
        inCompositeRoot = true;
      }
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
      if (Objects.equals(root, node.getTransform())) {
        inCompositeRoot = false;
      }
    }
    @Override
    public void visitTransform(TransformTreeNode node) {
      if (inCompositeRoot || Objects.equals(root, node.getTransform())) {
        displayData.add(DisplayData.from(node.getTransform()));
      }
    }

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {

    }
  }
}
