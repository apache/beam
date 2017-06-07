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

import static com.google.common.base.Preconditions.checkArgument;

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
  private final ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers;
  private final ListMultimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers;

  private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;
  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

  public static DirectGraph create(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
      ListMultimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    return new DirectGraph(
        producers, viewWriters, perElementConsumers, allConsumers, rootTransforms, stepNames);
  }

  private DirectGraph(
      Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers,
      Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters,
      ListMultimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
      ListMultimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers,
      Set<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
    this.producers = producers;
    this.viewWriters = viewWriters;
    this.perElementConsumers = perElementConsumers;
    this.allConsumers = allConsumers;
    this.rootTransforms = rootTransforms;
    this.stepNames = stepNames;
    for (AppliedPTransform<?, ?, ?> step : stepNames.keySet()) {
      for (PValue input : step.getInputs().values()) {
        checkArgument(
            allConsumers.get(input).contains(step),
            "Step %s lists value %s as input, but it is not in the graph of consumers",
            step.getFullName(),
            input);
      }
    }
  }

  AppliedPTransform<?, ?, ?> getProducer(PCollection<?> produced) {
    return producers.get(produced);
  }

  AppliedPTransform<?, ?, ?> getWriter(PCollectionView<?> view) {
    return viewWriters.get(view);
  }

  List<AppliedPTransform<?, ?, ?>> getPerElementConsumers(PValue consumed) {
    return perElementConsumers.get(consumed);
  }

  List<AppliedPTransform<?, ?, ?>> getAllConsumers(PValue consumed) {
    return allConsumers.get(consumed);
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
