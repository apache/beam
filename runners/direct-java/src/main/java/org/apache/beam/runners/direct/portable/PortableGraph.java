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

package org.apache.beam.runners.direct.portable;

import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.direct.ExecutableGraph;

/** A {@link ExecutableGraph} for a Portable {@link RunnerApi.Pipeline}. */
class PortableGraph implements ExecutableGraph<PTransformNode, PCollectionNode> {
  private final QueryablePipeline queryablePipeline;

  public static PortableGraph forPipeline(RunnerApi.Pipeline p) {
    return new PortableGraph(p);
  }

  private PortableGraph(RunnerApi.Pipeline p) {
    this.queryablePipeline =
        QueryablePipeline.forTransforms(p.getRootTransformIdsList(), p.getComponents());
  }

  @Override
  public Collection<PTransformNode> getRootTransforms() {
    return queryablePipeline.getRootTransforms();
  }

  @Override
  public Collection<PTransformNode> getExecutables() {
    return queryablePipeline.getTransforms();
  }

  @Override
  public PTransformNode getProducer(PCollectionNode collection) {
    return queryablePipeline.getProducer(collection);
  }

  @Override
  public Collection<PCollectionNode> getProduced(PTransformNode producer) {
    return queryablePipeline.getOutputPCollections(producer);
  }

  @Override
  public Collection<PCollectionNode> getPerElementInputs(PTransformNode transform) {
    return queryablePipeline.getPerElementInputPCollections(transform);
  }

  @Override
  public Collection<PTransformNode> getPerElementConsumers(PCollectionNode pCollection) {
    return queryablePipeline.getPerElementConsumers(pCollection);
  }
}
