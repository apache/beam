package org.beam.sdk.java.sql.planner;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamSQLRelUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSQLRelUtils.class);

  private static final AtomicInteger sequence = new AtomicInteger(0);
  private static final AtomicInteger classSequence = new AtomicInteger(0);

  public static String getStageName(BeamRelNode relNode) {
    return relNode.getClass().getSimpleName().toUpperCase() + "_" + relNode.getId() + "_"
        + sequence.getAndIncrement();
  }

  public static String getClassName(BeamRelNode relNode) {
    return "Generated_" + relNode.getClass().getSimpleName().toUpperCase() + "_" + relNode.getId()
        + "_" + classSequence.getAndIncrement();
  }

  public static BeamRelNode getBeamRelInput(RelNode input) {
    if (input instanceof RelSubset) {
      // go with known best input
      input = ((RelSubset) input).getBest();
    }
    return (BeamRelNode) input;
  }

  public static String explain(final RelNode rel) {
    return explain(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  public static String explain(final RelNode rel, SqlExplainLevel detailLevel) {
    String explain = "";
    try {
      explain = RelOptUtil.toString(rel);
    } catch (StackOverflowError e) {
      LOG.error(
          "StackOverflowError occurred while extracting plan. Please report it to the dev@ mailing list.");
      LOG.error("RelNode " + rel + " ExplainLevel " + detailLevel, e);
      LOG.error(
          "Forcing plan to empty string and continue... SQL Runner may not working properly after.");
    }
    return explain;
  }
}
