package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.runners.mapreduce.MapReduceRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Pipeline translator for {@link MapReduceRunner}.
 */
public class GraphConverter extends Pipeline.PipelineVisitor.Defaults {

  private final Map<PValue, TupleTag<?>> pValueToTupleTag;
  private final Map<TupleTag<?>, Graph.Vertex> outputToProducer;
  private final Graph graph;

  public GraphConverter() {
    this.pValueToTupleTag = Maps.newHashMap();
    this.outputToProducer = Maps.newHashMap();
    this.graph = new Graph();
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    Graph.Step step = Graph.Step.of(
        node.getFullName(),
        node.getTransform(),
        ImmutableList.copyOf(node.getInputs().keySet()),
        ImmutableList.copyOf(node.getOutputs().keySet()));
    Graph.Vertex v = graph.addVertex(step);

    for (PValue pValue : node.getInputs().values()) {
      TupleTag<?> tag = pValueToTupleTag.get(pValue);
      if (outputToProducer.containsKey(tag)) {
        Graph.Vertex producer = outputToProducer.get(tag);
        graph.addEdge(producer, v);
      }
    }

    for (Map.Entry<TupleTag<?>, PValue> entry : node.getOutputs().entrySet()) {
      pValueToTupleTag.put(entry.getValue(), entry.getKey());
      outputToProducer.put(entry.getKey(), v);
    }
  }

  public Graph getGraph() {
    return graph;
  }
}
