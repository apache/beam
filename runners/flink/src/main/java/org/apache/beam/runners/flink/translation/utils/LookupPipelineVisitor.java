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
package org.apache.beam.runners.flink.translation.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * Pipeline visitor that fills lookup table of {@link PTransform} to {@link AppliedPTransform} for
 * usage in {@link
 * org.apache.beam.runners.flink.FlinkBatchPortablePipelineTranslator.BatchTranslationContext}.
 */
public class LookupPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  private final Map<PTransform<?, ?>, AppliedPTransform<?, ?, ?>> lookupTable = new HashMap<>();

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    if (node.getTransform() != null) {
      final AppliedPTransform<?, ?, ?> applied = node.toAppliedPTransform(getPipeline());
      lookupTable.put(applied.getTransform(), applied);
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    final AppliedPTransform<?, ?, ?> applied = node.toAppliedPTransform(getPipeline());
    lookupTable.put(applied.getTransform(), applied);
  }

  private <InputT extends PInput, OutputT extends POutput>
      AppliedPTransform<InputT, OutputT, PTransform<InputT, OutputT>> applied(
          PTransform<InputT, OutputT> transform) {
    @SuppressWarnings("unchecked")
    final AppliedPTransform<InputT, OutputT, PTransform<InputT, OutputT>> applied =
        (AppliedPTransform<InputT, OutputT, PTransform<InputT, OutputT>>)
            lookupTable.get(transform);
    if (applied == null) {
      throw new IllegalArgumentException(
          String.format("AppliedPTransform for %s does not exist.", transform));
    }
    return applied;
  }

  public Map<TupleTag<?>, PCollection<?>> getInputs(PTransform<?, ?> transform) {
    return applied(transform).getInputs();
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(applied(transform)));
  }

  public Map<TupleTag<?>, PCollection<?>> getOutputs(PTransform<?, ?> transform) {
    return applied(transform).getOutputs();
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return (T) Iterables.getOnlyElement(applied(transform).getOutputs().values());
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, Coder<?>> getOutputCoders(PTransform<?, ?> transform) {
    return getOutputs(transform).entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
  }
}
