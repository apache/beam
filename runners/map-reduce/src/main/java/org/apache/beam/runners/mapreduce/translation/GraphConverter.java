package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.mapreduce.MapReduceRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;

/**
 * Pipeline translator for {@link MapReduceRunner}.
 */
public class GraphConverter extends Pipeline.PipelineVisitor.Defaults {

  private final Map<PValue, Graph.Vertex> outputToProducer;
  private final Graph graph;

  public GraphConverter() {
    this.outputToProducer = Maps.newHashMap();
    this.graph = new Graph();
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    Graph.Vertex v = graph.addVertex(node.getTransform());

    for (PValue input : node.getInputs().values()) {
      if (outputToProducer.containsKey(input)) {
        Graph.Vertex producer = outputToProducer.get(input);
        graph.addEdge(producer, v);
      }
    }

    for (PValue output : node.getOutputs().values()) {
      outputToProducer.put(output, v);
    }
  }

  public Graph getGraph() {
    return graph;
  }
}
