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

import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;

/**
 * A builder of simple {@link ExecutableGraph ExecutableGraphs} suitable for use in the portable
 * direct runner, to reduce verbosity of creating a graph with no payloads of any meaning.
 */
public class ExecutableGraphBuilder {
  private final RunnerApi.Components.Builder components;

  private ExecutableGraphBuilder() {
    components = Components.newBuilder();
  }

  public static ExecutableGraphBuilder create() {
    return new ExecutableGraphBuilder();
  }

  public ExecutableGraphBuilder addTransform(
      String name, @Nullable String input, String... outputs) {
    PTransform.Builder pt = PTransform.newBuilder().setUniqueName(name);
    if (input != null) {
      pt = pt.putInputs("input", input);
      addPCollection(input);
    }
    for (String output : outputs) {
      pt = pt.putOutputs(output, output);
      addPCollection(output);
    }
    components.putTransforms(name, pt.build());
    return this;
  }

  private ExecutableGraphBuilder addPCollection(String name) {
    components.putPcollections(name, PCollection.newBuilder().setUniqueName(name).build());
    return this;
  }

  public PTransformNode transformNode(String name) {
    return PipelineNode.pTransform(name, components.getTransformsOrThrow(name));
  }

  public PCollectionNode collectionNode(String name) {
    return PipelineNode.pCollection(name, components.getPcollectionsOrThrow(name));
  }

  public ExecutableGraph<PTransformNode, PCollectionNode> toGraph() {
    return PortableGraph.forPipeline(
        Pipeline.newBuilder()
            .setComponents(components)
            .addAllRootTransformIds(components.getTransformsMap().keySet())
            .build());
  }
}
