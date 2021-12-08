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
package org.apache.beam.runners.core.construction.graph;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** A {@link PipelineVisitor} to discover projection pushdown opportunities. */
class PushdownProjectorVisitor extends PipelineVisitor.Defaults {
  private final Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess;
  private final Map<
          ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
      pushdownOpportunities = new HashMap<>();

  /**
   * @param pCollectionFieldAccess A map from PCollection to the fields the pipeline accesses on
   *     that PCollection. See {@link FieldAccessVisitor}.
   */
  PushdownProjectorVisitor(Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess) {
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
    return ImmutableMap.copyOf(pushdownOpportunities);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    PTransform<?, ?> transform = node.getTransform();
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
    } else {
      pushdownOpportunities.put(pushdownProjector, localOpportunities);
      // If there are nested PushdownProjector implementations, apply only the outermost one.
      return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    }
  }
}
