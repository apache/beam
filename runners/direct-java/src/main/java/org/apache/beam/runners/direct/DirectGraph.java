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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;

/**
 * Methods for interacting with the underlying structure of a {@link Pipeline} that is being
 * executed with the {@link DirectRunner}.
 */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
class DirectGraph implements ExecutableGraph<AppliedPTransform<?, ?, ?>, PValue> {
  private final Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers;
  private final Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters;
  private final ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers;

  private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;
  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

  public static DirectGraph create(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    return new DirectGraph(producers, viewWriters, perElementConsumers, rootTransforms, stepNames);
  }

  private DirectGraph(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    this.producers = producers;
    this.viewWriters = viewWriters;
    this.perElementConsumers = perElementConsumers;
    this.rootTransforms = rootTransforms;
    this.stepNames = stepNames;
  }

  @Override
  public AppliedPTransform<?, ?, ?> getProducer(PValue produced) {
    if (produced instanceof PCollection) {
      return producers.get(produced);
    } else if (produced instanceof PCollectionView) {
      return getWriter((PCollectionView<?>) produced);
    }
    throw new IllegalArgumentException(
        String.format(
            "Unknown %s type %s. Known types: %s and %s",
            PValue.class.getSimpleName(),
            produced.getClass().getName(),
            PCollection.class.getSimpleName(),
            PCollectionView.class.getSimpleName()));
  }

  @Override
  public Collection<PValue> getProduced(AppliedPTransform<?, ?, ?> producer) {
    // TODO: This must only be called on primitive transforms; composites should return empty
    // values.
    return (Collection) producer.getOutputs().values();
  }

  @Override
  public Collection<PValue> getPerElementInputs(AppliedPTransform<?, ?, ?> transform) {
    // TODO: Make this actually track this type of edge, because this isn't quite the right
    // generic possibility, but is for ParDos
    return TransformInputs.nonAdditionalInputs(transform);
  }

  private AppliedPTransform<?, ?, ?> getWriter(PCollectionView<?> view) {
    return viewWriters.get(view);
  }

  @Override
  public List<AppliedPTransform<?, ?, ?>> getPerElementConsumers(PValue consumed) {
    return perElementConsumers.get(consumed);
  }

  @Override
  public Set<AppliedPTransform<?, ?, ?>> getRootTransforms() {
    return rootTransforms;
  }

  @Override
  public Collection<AppliedPTransform<?, ?, ?>> getExecutables() {
    return stepNames.keySet();
  }

  Set<PCollectionView<?>> getViews() {
    return viewWriters.keySet();
  }

  String getStepName(AppliedPTransform<?, ?, ?> step) {
    return stepNames.get(step);
  }
}
