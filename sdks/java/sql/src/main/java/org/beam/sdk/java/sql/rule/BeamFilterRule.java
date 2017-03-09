package org.beam.sdk.java.sql.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.beam.sdk.java.sql.rel.BeamFilterRel;
import org.beam.sdk.java.sql.rel.BeamLogicalConvention;

public class BeamFilterRule extends ConverterRule {
  public static BeamFilterRule INSTANCE = new BeamFilterRule();

  private BeamFilterRule() {
    super(LogicalFilter.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamFilterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final Filter filter = (Filter) rel;
    final RelNode input = filter.getInput();

    return new BeamFilterRel(filter.getCluster(),
        filter.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        filter.getCondition());
  }
}
