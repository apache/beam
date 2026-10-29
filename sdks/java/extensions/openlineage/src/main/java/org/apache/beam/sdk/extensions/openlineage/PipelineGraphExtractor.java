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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

/**
 * Walks the pipeline graph at submission time, feeding every transform through the {@link
 * VisitorFactory} and registering discovered datasets on the {@link OpenLineageContext}. Also
 * detects whether the pipeline is streaming (any unbounded {@link PCollection}), which drives the
 * {@code jobType} facet's processing type — the same heuristic the Flink integration's {@code
 * JobTypeUtils} applies to unbounded sources.
 */
class PipelineGraphExtractor {

  private PipelineGraphExtractor() {}

  static void extract(Pipeline pipeline, OpenLineageContext context) {
    VisitorFactory visitorFactory = new VisitorFactory();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            inspect(node);
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            inspect(node);
          }

          @Override
          public void visitValue(PValue value, TransformHierarchy.Node producer) {
            if (value instanceof PCollection
                && ((PCollection<?>) value).isBounded() == PCollection.IsBounded.UNBOUNDED) {
              context.setStreaming(true);
            }
          }

          private void inspect(TransformHierarchy.Node node) {
            PTransform<?, ?> transform = node.getTransform();
            if (transform == null) {
              return;
            }
            for (DatasetIdentifier dataset : visitorFactory.extractInputs(transform)) {
              context.registerDataset(OpenLineageContext.LineageDirection.INPUT, dataset);
            }
            for (DatasetIdentifier dataset : visitorFactory.extractOutputs(transform)) {
              context.registerDataset(OpenLineageContext.LineageDirection.OUTPUT, dataset);
            }
          }
        });
  }
}
