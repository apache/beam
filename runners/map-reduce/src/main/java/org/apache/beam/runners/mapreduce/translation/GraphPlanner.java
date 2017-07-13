package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by peihe on 06/07/2017.
 */
public class GraphPlanner {

  public Graph plan(Graph initGraph) {
    FusionVisitor fusionVisitor = new FusionVisitor();
    initGraph.accept(fusionVisitor);
    return fusionVisitor.getFusedGraph();
  }

  private class FusionVisitor implements GraphVisitor {

    private Graph fusedGraph;
    private Graph.Vertex workingVertex;
    private Graph.NodePath workingPath;

    FusionVisitor() {
      fusedGraph = new Graph();
      workingVertex = null;
      workingPath = null;
    }

    @Override
    public void visitRead(Graph.Vertex read) {
      if (workingVertex == null) {
        // drop if read is leaf vertex.
        return;
      }
      Graph.Vertex v = fusedGraph.addVertex(read.getTransform());
      workingPath.addFirst(read.getTransform());
      Graph.Edge edge = fusedGraph.addEdge(v, workingVertex);
      edge.addPath(workingPath);
    }

    @Override
    public void visitParDo(Graph.Vertex parDo) {
      checkArgument(
          parDo.getTransform().getAdditionalInputs().isEmpty(),
          "Side inputs are not supported.");
      if (workingVertex == null) {
        // Leaf vertex
        workingVertex = fusedGraph.addVertex(parDo.getTransform());
        workingPath = new Graph.NodePath();
      } else {
        workingPath.addFirst(parDo.getTransform());
      }
      checkArgument(
          parDo.getIncoming().size() == 1,
          "Side inputs are not supported.");
      processParent(parDo.getIncoming().iterator().next().getHead());
    }

    @Override
    public void visitFlatten(Graph.Vertex flatten) {
      if (workingVertex == null) {
        return;
      }
      Graph.NodePath basePath = workingPath;
      Graph.Vertex baseVertex = workingVertex;
      for (Graph.Edge e : flatten.getIncoming()) {
        workingPath = new Graph.NodePath(basePath);
        workingVertex = baseVertex;
        processParent(e.getHead());
      }
    }

    @Override
    public void visitGroupByKey(Graph.Vertex groupByKey) {
      if (workingVertex == null) {
        return;
      }
      Graph.Vertex v = fusedGraph.addVertex(groupByKey.getTransform());
      workingPath.addFirst(groupByKey.getTransform());
      Graph.Edge edge = fusedGraph.addEdge(v, workingVertex);
      edge.addPath(workingPath);
      processParent(groupByKey.getIncoming().iterator().next().getHead());
    }

    public Graph getFusedGraph() {
      return fusedGraph;
    }

    private void processParent(Graph.Vertex parent) {
      Graph.Vertex v = fusedGraph.getVertex(parent.getTransform());
      if (v == null) {
        parent.accept(this);
      } else {
        // TODO: parent is consumed more than once.
        // It is duplicated in multiple outgoing path. Figure out the impact.
        workingPath.addFirst(parent.getTransform());
        fusedGraph.getEdge(v, workingVertex).addPath(workingPath);
      }
    }
  }
}
