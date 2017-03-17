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
package org.apache.beam.runners.dataflow;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A mapping relating {@link Aggregator}s and the {@link PTransform} in which they are used.
 */
class DataflowAggregatorTransforms {
  private final Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorTransforms;
  private final Multimap<PTransform<?, ?>, AppliedPTransform<?, ?, ?>> transformAppliedTransforms;
  private final BiMap<AppliedPTransform<?, ?, ?>, String> appliedStepNames;

  public DataflowAggregatorTransforms(
      Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorTransforms,
      Map<AppliedPTransform<?, ?, ?>, String> transformStepNames) {
    this.aggregatorTransforms = aggregatorTransforms;
    appliedStepNames = HashBiMap.create(transformStepNames);

    transformAppliedTransforms = HashMultimap.create();
    for (AppliedPTransform<?, ?, ?> appliedTransform : transformStepNames.keySet()) {
      transformAppliedTransforms.put(appliedTransform.getTransform(), appliedTransform);
    }
  }

  /**
   * Returns true if the provided {@link Aggregator} is used in the constructing {@link Pipeline}.
   */
  public boolean contains(Aggregator<?, ?> aggregator) {
    return aggregatorTransforms.containsKey(aggregator);
  }

  /**
   * Gets the step names in which the {@link Aggregator} is used.
   */
  public Collection<String> getAggregatorStepNames(Aggregator<?, ?> aggregator) {
    Collection<String> names = new HashSet<>();
    Collection<PTransform<?, ?>> transforms = aggregatorTransforms.get(aggregator);
    for (PTransform<?, ?> transform : transforms) {
      for (AppliedPTransform<?, ?, ?> applied : transformAppliedTransforms.get(transform)) {
        names.add(appliedStepNames.get(applied));
      }
    }
    return names;
  }

  /**
   * Gets the {@link PTransform} that was assigned the provided step name.
   */
  public AppliedPTransform<?, ?, ?> getAppliedTransformForStepName(String stepName) {
    return appliedStepNames.inverse().get(stepName);
  }
}
