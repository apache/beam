package org.beam.sdk.java.sql.rel;

import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;
import org.beam.sdk.java.sql.planner.BeamSQLRelUtils;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

import com.google.common.base.Joiner;

public class BeamIOSourceRel extends TableScan implements BeamRelNode {

  public BeamIOSourceRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
  }

  @Override
  public void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {

    String sourceName = Joiner.on('.').join(getTable().getQualifiedName()).replace(".(STREAM)", "");

    BaseBeamTable sourceTable = planCreator.getKafkaTables().get(sourceName);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> sourceStream = planCreator.getPipeline()
        .apply(stageName,  sourceTable.buildReadTransform());
    PCollection<BeamSQLRow> reformattedSourceStream = sourceStream.apply("sourceReformat", sourceTable.getSourceConverter());

    planCreator.setLatestStream(reformattedSourceStream);

    System.out.println("Build: add_source " + sourceName);
  }

}
