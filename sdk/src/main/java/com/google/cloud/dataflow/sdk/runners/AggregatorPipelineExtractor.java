/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.AggregatorRetriever;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Retrieves {@link Aggregator Aggregators} at each {@link ParDo} and returns a {@link Map} of
 * {@link Aggregator} to the {@link PTransform PTransforms} in which it is present.
 */
public class AggregatorPipelineExtractor {
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

  private static class AggregatorVisitor implements PipelineVisitor {
    private final SetMultimap<Aggregator<?, ?>, PTransform<?, ?>> aggregatorSteps;

    public AggregatorVisitor(SetMultimap<Aggregator<?, ?>, PTransform<?, ?>> aggregatorSteps) {
      this.aggregatorSteps = aggregatorSteps;
    }

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {}

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {}

    @Override
    public void visitTransform(TransformTreeNode node) {
      PTransform<?, ?> transform = node.getTransform();
      addStepToAggregators(transform, getAggregators(transform));
    }

    private Collection<Aggregator<?, ?>> getAggregators(PTransform<?, ?> transform) {
      if (transform != null) {
        if (transform instanceof ParDo.Bound) {
          return AggregatorRetriever.getAggregators(((ParDo.Bound<?, ?>) transform).getFn());
        } else if (transform instanceof ParDo.BoundMulti) {
          return AggregatorRetriever.getAggregators(((ParDo.BoundMulti<?, ?>) transform).getFn());
        }
      }
      return Collections.emptyList();
    }

    private void addStepToAggregators(
        PTransform<?, ?> transform, Collection<Aggregator<?, ?>> aggregators) {
      for (Aggregator<?, ?> aggregator : aggregators) {
        aggregatorSteps.put(aggregator, transform);
      }
    }

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {}
  }
}
