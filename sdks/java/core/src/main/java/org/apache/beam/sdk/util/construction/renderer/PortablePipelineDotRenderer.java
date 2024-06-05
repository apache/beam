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
package org.apache.beam.sdk.util.construction.renderer;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;

/**
 * A DOT renderer for BEAM portable {@link org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class PortablePipelineDotRenderer {
  private final StringBuilder dotBuilder = new StringBuilder();
  private final Map<String, Integer> valueToProducerNodeId = new HashMap<>();
  private int indent;
  private int nextNodeId;

  private PortablePipelineDotRenderer() {}

  static String toDotString(RunnerApi.Pipeline pipeline) {
    return new PortablePipelineDotRenderer().toDot(pipeline);
  }

  private String toDot(RunnerApi.Pipeline pipeline) {
    final QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());

    begin();

    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      visitTransform(transform);
    }

    end();

    return dotBuilder.toString();
  }

  private void visitTransform(PipelineNode.PTransformNode node) {
    final int nodeId = nextNodeId++;
    final RunnerApi.PTransform transform = node.getTransform();
    writeLine(
        "%d [label=\"%s\\n%s\"]",
        nodeId,
        escapeString(transform.getUniqueName()),
        escapeString(transform.getSpec().getUrn()));

    transform.getOutputsMap().values().forEach(x -> valueToProducerNodeId.put(x, nodeId));

    transform
        .getInputsMap()
        .forEach(
            (key, value) -> {
              final int producerId = valueToProducerNodeId.get(value);
              String style = "solid";
              writeLine(
                  "%d -> %d [style=%s label=\"%s\"]",
                  producerId,
                  nodeId,
                  style,
                  escapeString(value.substring(value.lastIndexOf('_') + 1)));
            });
  }

  private void begin() {
    writeLine("digraph {");
    enterBlock();
    writeLine("rankdir=LR");
  }

  private void end() {
    exitBlock();
    writeLine("}");
  }

  private void enterBlock() {
    indent += 4;
  }

  private void exitBlock() {
    indent -= 4;
  }

  @FormatMethod
  private void writeLine(@FormatString String format, Object... args) {
    if (indent != 0) {
      dotBuilder.append(String.format("%-" + indent + "s", ""));
    }
    dotBuilder.append(String.format(format, args));
    dotBuilder.append(System.lineSeparator());
  }

  private static String escapeString(String x) {
    return x.replace("\"", "\\\"");
  }
}
