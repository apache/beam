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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/**
 * Validate correct implementation of {@link DisplayData} by evaluating {@link
 * HasDisplayData#populateDisplayData(DisplayData.Builder)} during pipeline construction.
 */
class DisplayDataValidator {
  // Do not instantiate
  private DisplayDataValidator() {}

  static void validatePipeline(Pipeline pipeline) {
    validateTransforms(pipeline);
  }

  static void validateOptions(PipelineOptions options) {
    evaluateDisplayData(options);
  }

  private static void validateTransforms(Pipeline pipeline) {
    pipeline.traverseTopologically(Visitor.INSTANCE);
  }

  private static void evaluateDisplayData(HasDisplayData component) {
    DisplayData.from(component);
  }

  private static class Visitor extends Pipeline.PipelineVisitor.Defaults {
    private static final Visitor INSTANCE = new Visitor();

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      if (!node.isRootNode()) {
        evaluateDisplayData(node.getTransform());
      }

      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      evaluateDisplayData(node.getTransform());
    }
  }
}
