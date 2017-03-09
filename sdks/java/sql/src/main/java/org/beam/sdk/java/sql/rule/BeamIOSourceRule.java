package org.beam.sdk.java.sql.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.beam.sdk.java.sql.rel.BeamIOSourceRel;
import org.beam.sdk.java.sql.rel.BeamLogicalConvention;

public class BeamIOSourceRule extends ConverterRule {
  public static final BeamIOSourceRule INSTANCE = new BeamIOSourceRule();

  private BeamIOSourceRule() {
    super(LogicalTableScan.class, Convention.NONE, BeamLogicalConvention.INSTANCE,
        "BeamIOSourceRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final TableScan scan = (TableScan) rel;

    return new BeamIOSourceRel(scan.getCluster(),
        scan.getTraitSet().replace(BeamLogicalConvention.INSTANCE), scan.getTable());
  }

}
