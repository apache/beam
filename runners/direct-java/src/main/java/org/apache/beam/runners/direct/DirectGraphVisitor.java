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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Tracks the {@link AppliedPTransform AppliedPTransforms} that consume each {@link PValue} in the
 * {@link Pipeline}. This is used to schedule consuming {@link PTransform PTransforms} to consume
 * input after the upstream transform has produced and committed output.
 */
class DirectGraphVisitor extends PipelineVisitor.Defaults {

  private static final Set<Class<? extends PTransform>> PRODUCES_KEYED_OUTPUTS = ImmutableSet.of(
      SplittableParDo.GBKIntoKeyedWorkItems.class, DirectGroupByKeyOnly.class,
      DirectGroupAlsoByWindow.class);

  private Map<POutput, AppliedPTransform<?, ?, ?>> producers = new HashMap<>();

  private ListMultimap<PInput, AppliedPTransform<?, ?, ?>> primitiveConsumers =
      ArrayListMultimap.create();

  private Set<PCollectionView<?>> views = new HashSet<>();
  private Set<AppliedPTransform<?, ?, ?>> rootTransforms = new HashSet<>();
  private Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap<>();
  private Set<PValue> keyedValues = new HashSet<>();

  private int numTransforms = 0;
  private boolean finalized = false;

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s "
            + "which has already visited a Pipeline and is finalized",
        node.getFullName(),
        getClass().getSimpleName());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s which is already finalized",
        node.getFullName(),
        getClass().getSimpleName());
    if (node.isRootNode()) {
      finalized = true;
    } else if (PRODUCES_KEYED_OUTPUTS.contains(node.getTransform().getClass())) {
      Map<TupleTag<?>, PValue> outputs = node.getOutputs();
      for (PValue output : outputs.values()) {
        keyedValues.add(output);
      }
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    AppliedPTransform<?, ?, ?> appliedTransform = getAppliedTransform(node);
    stepNames.put(appliedTransform, genStepName());
    if (node.getInputs().isEmpty()) {
      rootTransforms.add(appliedTransform);
    } else {
      for (PValue value : node.getInputs().values()) {
        primitiveConsumers.put(value, appliedTransform);
      }
    }
  }

 @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    AppliedPTransform<?, ?, ?> appliedTransform = getAppliedTransform(producer);
    if (value instanceof PCollectionView) {
      views.add((PCollectionView<?>) value);
    }
    if (!producers.containsKey(value)) {
      producers.put(value, appliedTransform);
    }
    boolean inputsAreKeyed = true;
    for (PValue input : producer.getInputs().values()) {
      inputsAreKeyed = inputsAreKeyed && keyedValues.contains(input);
    }
    if (PRODUCES_KEYED_OUTPUTS.contains(producer.getTransform().getClass())
        || (isKeyPreserving(producer.getTransform()) && inputsAreKeyed)) {
      keyedValues.add(value);
    }
  }

  private AppliedPTransform<?, ?, ?> getAppliedTransform(TransformHierarchy.Node node) {
    @SuppressWarnings({"rawtypes", "unchecked"})
    AppliedPTransform<?, ?, ?> application = node.toAppliedPTransform();
    return application;
  }

  private String genStepName() {
    return String.format("s%s", numTransforms++);
  }

  /**
   * Get the graph constructed by this {@link DirectGraphVisitor}, which provides
   * lookups for producers and consumers of {@link PValue PValues}.
   */
  DirectGraph getGraph() {
    checkState(finalized, "Can't get a graph before the Pipeline has been completely traversed");
    return DirectGraph.create(producers, primitiveConsumers, views, rootTransforms, stepNames);
  }

  /**
   * Get all keyed {@link PValue PValues}. A {@link PValue} is keyed if it
   * is the result of a {@link PTransform} that produces keyed outputs. A {@link PTransform} that
   * produces keyed outputs is assumed to colocate output elements that share a key.
   *
   * <p>All {@link GroupByKey} transforms, or their runner-specific implementation primitive,
   * produce keyed output.
   * TODO: Handle Key-preserving transforms when appropriate and more aggressively make PTransforms
   * unkeyed
   */
  Set<PValue> getKeyedPValues() {
    checkState(finalized,
        "can't call getKeyedPValues before a Pipeline has been completely traversed");
    return keyedValues;
  }

  private static boolean isKeyPreserving(PTransform<?, ?> transform) {
    // This is a hacky check for what is considered key-preserving to the direct runner.
    // The most obvious alternative would be a package-private marker interface, but
    // better to make this obviously hacky so it is less likely to proliferate. Meanwhile
    // we intend to allow explicit expression of key-preserving DoFn in the model.
    if (transform instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput<?, ?> parDo = (ParDo.MultiOutput<?, ?>) transform;
      return parDo.getFn() instanceof ParDoMultiOverrideFactory.ToKeyedWorkItem;
    } else {
      return false;
    }
  }

}
