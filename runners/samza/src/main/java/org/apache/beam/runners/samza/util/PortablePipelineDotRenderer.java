package org.apache.beam.runners.direct.portable;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;

/**
 * A DOT renderer for BEAM portable {@link org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline}.
 */
public class PortablePipelineDotRenderer {
  private final StringBuilder dotBuilder = new StringBuilder();
  private final Map<String, Integer> valueToProducerNodeId = new HashMap<>();
  private int indent;
  private int nextNodeId;

  public static String toDotString(RunnerApi.Pipeline pipeline) {
    final PortablePipelineDotRenderer renderer = new PortablePipelineDotRenderer();
    return renderer.toDot(pipeline);
  }

  private PortablePipelineDotRenderer() {}

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
    transform
        .getOutputs()
        .values()
        .forEach(
            x -> {
              valueToProducerNodeId.put(x, nodeId);
              writeLine(
                  "%d [label=\"%s\\n%s\"]",
                  nodeId,
                  escapeString(transform.getUniqueName()),
                  escapeString(transform.getSpec().getUrn()));
            });

    transform
        .getInputs()
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

  private void writeLine(String format, Object... args) {
    if (indent != 0) {
      dotBuilder.append(String.format("%-" + indent + "s", ""));
    }
    dotBuilder.append(String.format(format, args));
    dotBuilder.append("\n");
  }

  private static String escapeString(String x) {
    return x.replace("\"", "\\\"");
  }

  private static String shortenTag(String tag) {
    return tag.replaceFirst(".*:([a-zA-Z#0-9]+).*", "$1");
  }
}
