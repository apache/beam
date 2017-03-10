package org.beam.sdk.java.sql.rel;

import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;
import org.beam.sdk.java.sql.planner.BeamSQLRelUtils;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

import com.google.common.base.Joiner;

public class BeamIOSinkRel extends TableModify implements BeamRelNode {
  public BeamIOSinkRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
      Prepare.CatalogReader catalogReader, RelNode child, Operation operation,
      List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamIOSinkRel(getCluster(), traitSet, getTable(), getCatalogReader(),
        sole(inputs), getOperation(), getUpdateColumnList(), getSourceExpressionList(),
        isFlattened());
  }

  @Override
  public void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {

    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> upstream = planCreator.getLatestStream();

    String sourceName = Joiner.on('.').join(getTable().getQualifiedName());

    BaseBeamTable targetTable = planCreator.getSourceTables().get(sourceName);

    PCollection formattedOutput = upstream.apply("preformat_to_target", targetTable.getSinkConcerter());
    formattedOutput.apply("persistent", targetTable.buildWriteTransform());

    planCreator.setHasPersistent(true);

    System.out.println("Build: add_persistent " + stageName);
  }


}
