package org.beam.sdk.java.sql.rel;

import org.apache.calcite.rel.RelNode;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;

/**
 * A new method {@link #buildBeamPipeline(BeamPipelineCreator)} is added, it's called by {@link BeamPipelineCreator}.
 *
 */
public interface BeamRelNode extends RelNode {
  
  /**
   * A {@link BeamRelNode} is a recursive structure, the {@link BeamPipelineCreator} visits it with a DFS(Depth-First-Search) algorithm.
   * @param planCreator
   * @throws Exception
   */
  void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception;
}
