package org.beam.sdk.java.sql.rel;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.beam.sdk.java.sql.interpreter.BeamSQLExpressionExecutor;
import org.beam.sdk.java.sql.interpreter.BeamSQLSpELExecutor;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;
import org.beam.sdk.java.sql.planner.BeamSQLRelUtils;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.beam.sdk.java.sql.transform.BeamSQLFilterFn;

public class BeamFilterRel extends Filter implements BeamRelNode {

  public BeamFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
      RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new BeamFilterRel(getCluster(), traitSet, input, condition);
  }
  
  @Override
  public void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {

    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> upstream = planCreator.getLatestStream();

    BeamSQLExpressionExecutor executor = new BeamSQLSpELExecutor(this);

    PCollection<BeamSQLRow> projectStream = upstream.apply(stageName,
        ParDo.of(new BeamSQLFilterFn(getRelTypeName(), executor)));

    planCreator.setLatestStream(projectStream);

    System.out.println("Build: apply_filter " + stageName );
  }


}
