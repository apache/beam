package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.COMPARISON;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;

public class TestTableFilter implements BeamSqlTableFilter {
  private List<RexNode> supported;
  private List<RexNode> unsupported;

  public TestTableFilter(List<RexNode> predicateCNF) {
    supported = new ArrayList<>();
    unsupported = new ArrayList<>();

    for (RexNode node : predicateCNF) {
      if (isSupported(node)) {
        supported.add(node);
      } else {
        unsupported.add(node);
      }
    }
  }

  @Override
  public List<RexNode> getNotSupported() {
    return unsupported;
  }

  public List<RexNode> getSupported() {
    return supported;
  }

  /**
   * Check whether a {@code RexNode} is supported. For testing purposes only simple nodes are supported. Ex: comparison between 2 input fields, input field to a literal, literal to a literal.
   * @param node A node to check for predicate push-down support.
   * @return True when a node is supported, false otherwise.
   */
  private boolean isSupported(RexNode node) {
    if (node instanceof RexCall) {
      RexCall compositeNode = (RexCall) node;

      // Only support comparisons in a predicate
      if (!node.getKind().belongsTo(COMPARISON)) {
        return false;
      }

      for (RexNode operand : compositeNode.getOperands()) {
        if (!(operand instanceof RexLiteral) && !(operand instanceof RexInputRef)) {
          return false;
        }
      }
    } else {
      throw new RuntimeException("Encountered an unexpected node type: " + node.getClass().getSimpleName());
    }

    return true;
  }
}
