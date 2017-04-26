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
package org.apache.beam.sdk;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PValue;

/**
 * Retrieves {@link Aggregator Aggregators} at each {@link ParDo} and returns a {@link Map} of
 * {@link Aggregator} to the {@link PTransform PTransforms} in which it is present.
 */
@Deprecated
class AggregatorPipelineExtractor {
  private final Pipeline pipeline;

  /**
   * Creates an {@code AggregatorPipelineExtractor} for the given {@link Pipeline}.
   */
  public AggregatorPipelineExtractor(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * Returns a {@link Map} between each {@link Aggregator} in the {@link Pipeline} to the {@link
   * PTransform PTransforms} in which it is used.
   */
  public Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> getAggregatorSteps() {
    HashMultimap<Aggregator<?, ?>, PTransform<?, ?>> aggregatorSteps = HashMultimap.create();
    pipeline.traverseTopologically(new AggregatorVisitor(aggregatorSteps));
    return aggregatorSteps.asMap();
  }

  private static class AggregatorVisitor extends PipelineVisitor.Defaults {
    private final SetMultimap<Aggregator<?, ?>, PTransform<?, ?>> aggregatorSteps;

    public AggregatorVisitor(SetMultimap<Aggregator<?, ?>, PTransform<?, ?>> aggregatorSteps) {
      this.aggregatorSteps = aggregatorSteps;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      PTransform<?, ?> transform = node.getTransform();
      addStepToAggregators(transform, getAggregators(transform));
    }

    private Collection<Aggregator<?, ?>> getAggregators(PTransform<?, ?> transform) {
      return Collections.emptyList();
    }

    private void addStepToAggregators(
        PTransform<?, ?> transform, Collection<Aggregator<?, ?>> aggregators) {
      for (Aggregator<?, ?> aggregator : aggregators) {
        aggregatorSteps.put(aggregator, transform);
      }
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {}
  }
}
