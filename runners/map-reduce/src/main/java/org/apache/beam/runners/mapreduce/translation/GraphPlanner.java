package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.Coder;

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
    private Coder<?> workingEdgeCoder;

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
      Graph.Vertex v = fusedGraph.addVertex(read.getStep());
      workingPath.addFirst(read.getStep());
      Graph.Edge edge = fusedGraph.addEdge(v, workingVertex, workingEdgeCoder);
      edge.addPath(workingPath);
    }

    @Override
    public void visitParDo(Graph.Vertex parDo) {
      Graph.Step step = parDo.getStep();
      checkArgument(
          step.getTransform().getAdditionalInputs().isEmpty(),
          "Side inputs are not " + "supported.");
      checkArgument(
          parDo.getIncoming().size() == 1,
          "Side inputs are not supported.");
      Graph.Edge inEdge = parDo.getIncoming().iterator().next();

      if (workingVertex == null) {
        // Leaf vertex
        workingVertex = fusedGraph.addVertex(step);
        workingPath = new Graph.NodePath();
        workingEdgeCoder = inEdge.getCoder();
      } else {
        workingPath.addFirst(step);
      }
      processParent(inEdge.getHead());
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
        workingEdgeCoder = e.getCoder();
        processParent(e.getHead());
      }
    }

    @Override
    public void visitGroupByKey(Graph.Vertex groupByKey) {
      if (workingVertex == null) {
        return;
      }
      Graph.Step step = groupByKey.getStep();
      Graph.Vertex addedGroupByKey = fusedGraph.addVertex(step);

      Graph.Edge edge = fusedGraph.addEdge(
          addedGroupByKey,
          workingVertex,
          workingEdgeCoder);
      edge.addPath(workingPath);
      Graph.Edge inEdge = groupByKey.getIncoming().iterator().next();
      workingVertex = addedGroupByKey;
      workingPath = new Graph.NodePath();
      workingEdgeCoder = inEdge.getCoder();
      processParent(inEdge.getHead());
    }

    public Graph getFusedGraph() {
      return fusedGraph;
    }

    private void processParent(Graph.Vertex parent) {
      Graph.Step step = parent.getStep();
      Graph.Vertex v = fusedGraph.getVertex(step);
      if (v == null) {
        parent.accept(this);
      } else {
        // TODO: parent is consumed more than once.
        // It is duplicated in multiple outgoing path. Figure out the impact.
        workingPath.addFirst(step);
        fusedGraph.getEdge(v, workingVertex).addPath(workingPath);
      }
    }
  }
}
