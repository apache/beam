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

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;

/**
 * Tracks the {@link AppliedPTransform AppliedPTransforms} that consume each {@link PValue} in the
 * {@link Pipeline}. This is used to schedule consuming {@link PTransform PTransforms} to consume
 * input after the upstream transform has produced and committed output.
 */
public class ConsumerTrackingPipelineVisitor extends PipelineVisitor.Defaults {
  private Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers = new HashMap<>();
  private Collection<AppliedPTransform<?, ?, ?>> rootTransforms = new ArrayList<>();
  private Collection<PCollectionView<?>> views = new ArrayList<>();
  private Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap<>();
  private Set<PValue> toFinalize = new HashSet<>();
  private int numTransforms = 0;
  private boolean finalized = false;

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s "
            + "which has already visited a Pipeline and is finalized",
        node.getFullName(),
        ConsumerTrackingPipelineVisitor.class.getSimpleName());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s which is already finalized",
        node.getFullName(),
        ConsumerTrackingPipelineVisitor.class.getSimpleName());
    if (node.isRootNode()) {
      finalized = true;
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
    toFinalize.removeAll(node.getInput().expand());
    AppliedPTransform<?, ?, ?> appliedTransform = getAppliedTransform(node);
    stepNames.put(appliedTransform, genStepName());
    if (node.getInput().expand().isEmpty()) {
      rootTransforms.add(appliedTransform);
    } else {
      for (PValue value : node.getInput().expand()) {
        valueToConsumers.get(value).add(appliedTransform);
      }
    }
  }

  private AppliedPTransform<?, ?, ?> getAppliedTransform(TransformTreeNode node) {
    @SuppressWarnings({"rawtypes", "unchecked"})
    AppliedPTransform<?, ?, ?> application = AppliedPTransform.of(
        node.getFullName(), node.getInput(), node.getOutput(), (PTransform) node.getTransform());
    return application;
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    toFinalize.add(value);
    for (PValue expandedValue : value.expand()) {
      valueToConsumers.put(expandedValue, new ArrayList<AppliedPTransform<?, ?, ?>>());
      if (expandedValue instanceof PCollectionView) {
        views.add((PCollectionView<?>) expandedValue);
      }
      expandedValue.recordAsOutput(getAppliedTransform(producer));
    }
    value.recordAsOutput(getAppliedTransform(producer));
  }

  private String genStepName() {
    return String.format("s%s", numTransforms++);
  }


  /**
   * Returns a mapping of each fully-expanded {@link PValue} to each
   * {@link AppliedPTransform} that consumes it. For each AppliedPTransform in the collection
   * returned from {@code getValueToCustomers().get(PValue)},
   * {@code AppliedPTransform#getInput().expand()} will contain the argument {@link PValue}.
   */
  public Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> getValueToConsumers() {
    checkState(
        finalized,
        "Can't call getValueToConsumers before the Pipeline has been completely traversed");

    return valueToConsumers;
  }

  /**
   * Returns the mapping for each {@link AppliedPTransform} in the {@link Pipeline} to a unique step
   * name.
   */
  public Map<AppliedPTransform<?, ?, ?>, String> getStepNames() {
    checkState(
        finalized, "Can't call getStepNames before the Pipeline has been completely traversed");

    return stepNames;
  }

  /**
   * Returns the root transforms of the {@link Pipeline}. A root {@link AppliedPTransform} consumes
   * a {@link PInput} where the {@link PInput#expand()} returns an empty collection.
   */
  public Collection<AppliedPTransform<?, ?, ?>> getRootTransforms() {
    checkState(
        finalized,
        "Can't call getRootTransforms before the Pipeline has been completely traversed");

    return rootTransforms;
  }

  /**
   * Returns all of the {@link PCollectionView PCollectionViews} contained in the visited
   * {@link Pipeline}.
   */
  public Collection<PCollectionView<?>> getViews() {
    checkState(finalized, "Can't call getViews before the Pipeline has been completely traversed");

    return views;
  }

  /**
   * Returns all of the {@link PValue PValues} that have been produced but not consumed. These
   * {@link PValue PValues} should be finalized by the {@link PipelineRunner} before the
   * {@link Pipeline} is executed.
   */
  public Set<PValue> getUnfinalizedPValues() {
    checkState(
        finalized,
        "Can't call getUnfinalizedPValues before the Pipeline has been completely traversed");

    return toFinalize;
  }
}
