package org.apache.beam.runners.mapreduce.translation;

/**
 * Created by peihe on 06/07/2017.
 */
public interface GraphVisitor {
  void visitRead(Graph.Vertex read);
  void visitParDo(Graph.Vertex parDo);
  void visitFlatten(Graph.Vertex flatten);
  void visitGroupByKey(Graph.Vertex groupByKey);
}
