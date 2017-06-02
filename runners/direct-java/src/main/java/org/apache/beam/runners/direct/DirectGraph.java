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

import com.google.common.collect.ListMultimap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;

/**
 * Methods for interacting with the underlying structure of a {@link Pipeline} that is being
 * executed with the {@link DirectRunner}.
 */
class DirectGraph {
  private final Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers;
  private final Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters;
  private final ListMultimap<PInput, AppliedPTransform<?, ?, ?>> primitiveConsumers;

  private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;
  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

  public static DirectGraph create(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> primitiveConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    return new DirectGraph(producers, viewWriters, primitiveConsumers, rootTransforms, stepNames);
  }

  private DirectGraph(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> primitiveConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    this.producers = producers;
    this.viewWriters = viewWriters;
    this.primitiveConsumers = primitiveConsumers;
    this.rootTransforms = rootTransforms;
    this.stepNames = stepNames;
  }

  AppliedPTransform<?, ?, ?> getProducer(PCollection<?> produced) {
    return producers.get(produced);
  }

  AppliedPTransform<?, ?, ?> getWriter(PCollectionView<?> view) {
    return viewWriters.get(view);
  }

  List<AppliedPTransform<?, ?, ?>> getPrimitiveConsumers(PValue consumed) {
    return primitiveConsumers.get(consumed);
  }

  Set<AppliedPTransform<?, ?, ?>> getRootTransforms() {
    return rootTransforms;
  }

  Set<PCollection<?>> getPCollections() {
    return producers.keySet();
  }

  Set<PCollectionView<?>> getViews() {
    return viewWriters.keySet();
  }

  String getStepName(AppliedPTransform<?, ?, ?> step) {
    return stepNames.get(step);
  }

  Collection<AppliedPTransform<?, ?, ?>> getPrimitiveTransforms() {
    return stepNames.keySet();
  }
}
