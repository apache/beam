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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;

/** A DOT renderer for BEAM {@link Pipeline} DAG. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PipelineDotRenderer implements Pipeline.PipelineVisitor {
  public static String toDotString(Pipeline pipeline) {
    final PipelineDotRenderer visitor = new PipelineDotRenderer();
    visitor.begin();
    pipeline.traverseTopologically(visitor);
    visitor.end();
    return visitor.dotBuilder.toString();
  }

  public static String toDotString(RunnerApi.Pipeline pipeline) {
    return PortablePipelineDotRenderer.toDotString(pipeline);
  }

  private final StringBuilder dotBuilder = new StringBuilder();
  private final Map<PValue, Integer> valueToProducerNodeId = new HashMap<>();

  private int indent;
  private int nextNodeId;

  private PipelineDotRenderer() {}

  @Override
  public void enterPipeline(Pipeline p) {}

  @Override
  public void leavePipeline(Pipeline pipeline) {}

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    writeLine("subgraph cluster_%d {", nextNodeId++);
    enterBlock();
    writeLine("label = \"%s\"", escapeString(node.getFullName()));
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    exitBlock();
    writeLine("}");
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    final int nodeId = nextNodeId++;
    writeLine("%d [label=\"%s\"]", nodeId, escapeString(node.getTransform().getName()));

    node.getOutputs().values().forEach(x -> valueToProducerNodeId.put(x, nodeId));

    node.getInputs()
        .forEach(
            (key, value) -> {
              final int producerId = valueToProducerNodeId.get(value);
              String style = "solid";
              if (node.getTransform().getAdditionalInputs().containsKey(key)) {
                style = "dashed";
              }
              writeLine("%d -> %d [style=%s label=\"%s\"]", producerId, nodeId, style, "");
            });
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {}

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
