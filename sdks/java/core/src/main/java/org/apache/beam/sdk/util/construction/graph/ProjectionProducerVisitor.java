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
package org.apache.beam.sdk.util.construction.graph;

import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** A {@link PipelineVisitor} to discover projection pushdown opportunities. */
class ProjectionProducerVisitor extends PipelineVisitor.Defaults {
  private final Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess;
  private final ImmutableMap.Builder<
          ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
      pushdownOpportunities = ImmutableMap.builder();

  /**
   * @param pCollectionFieldAccess A map from PCollection to the fields the pipeline accesses on
   *     that PCollection. See {@link FieldAccessVisitor}.
   */
  ProjectionProducerVisitor(Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess) {
    this.pCollectionFieldAccess = pCollectionFieldAccess;
  }

  /**
   * A map from {@link ProjectionProducer} to an inner map keyed by output PCollections of that
   * producer. For each PCollection, the value is the {@link FieldAccessDescriptor} describing the
   * smallest possible subset of fields the producer is required to return on that PCollection. If
   * there is no proper subset, the result is not included.
   */
  Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
      getPushdownOpportunities() {
    return pushdownOpportunities.build();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    PTransform<?, ?> transform = node.getTransform();

    // TODO(https://github.com/apache/beam/issues/21359) Support inputs other than PBegin.
    if (!node.getInputs().isEmpty()) {
      return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    }

    if (!(transform instanceof ProjectionProducer)) {
      return CompositeBehavior.ENTER_TRANSFORM;
    }
    ProjectionProducer<PTransform<?, ?>> pushdownProjector =
        (ProjectionProducer<PTransform<?, ?>>) transform;
    if (!pushdownProjector.supportsProjectionPushdown()) {
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    ImmutableMap.Builder<PCollection<?>, FieldAccessDescriptor> builder = ImmutableMap.builder();
    for (PCollection<?> output : node.getOutputs().values()) {
      FieldAccessDescriptor fieldAccess = pCollectionFieldAccess.get(output);
      if (fieldAccess != null && !fieldAccess.getAllFields()) {
        builder.put(output, fieldAccess);
      }
    }
    Map<PCollection<?>, FieldAccessDescriptor> localOpportunities = builder.build();
    if (localOpportunities.isEmpty()) {
      return CompositeBehavior.ENTER_TRANSFORM;
    }
    pushdownOpportunities.put(pushdownProjector, localOpportunities);
    // If there are nested PushdownProjector implementations, apply only the outermost one.
    return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
  }
}
