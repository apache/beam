package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.BETWEEN;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.CAST;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.DIVIDE;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.MINUS;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.MOD;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.OR;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.PLUS;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.TIMES;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;

public class BigQueryFilter implements BeamSqlTableFilter {
  private static final ImmutableSet<SqlKind> SUPPORTED_OPS = ImmutableSet.<SqlKind>builder()
      .add(COMPARISON.toArray(new SqlKind[0]))
      // TODO: Check what other functions are supported and add support for them (ex: trim).
      .add(PLUS, MINUS, MOD, DIVIDE, TIMES, LIKE, BETWEEN, CAST).build();
  private List<RexNode> supported;
  private List<RexNode> unsupported;

  public BigQueryFilter(List<RexNode> predicateCNF) {
    supported = new ArrayList<>();
    unsupported = new ArrayList<>();

    for (RexNode node : predicateCNF) {
      if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        throw new RuntimeException(
            "Predicate node '"
                + node.getClass().getSimpleName()
                + "' should be a boolean expression, but was: "
                + node.getType().getSqlTypeName());
      }

      if (isSupported(node).getLeft()) {
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

  @Override
  public String toString() {
    String supStr =
        "supported{"
            + supported.stream().map(RexNode::toString).collect(Collectors.joining())
            + "}";
    String unsupStr =
        "unsupported{"
            + unsupported.stream().map(RexNode::toString).collect(Collectors.joining())
            + "}";

    return "[" + supStr + ", " + unsupStr + "]";
  }

  /**
   * Check whether a {@code RexNode} is supported. As of right now BigQuery supports:
   * 1. Complex predicates (both conjunction and disjunction).
   * 2. Comparison between a column and a literal.
   *
   * TODO: Check if comparison between two columns is supported. Also over a boolean field.
   *
   * @param node A node to check for predicate push-down support.
   * @return A pair containing a boolean whether an expression is supported and the number of input references used by the expression.
   */
  private Pair<Boolean, Integer> isSupported(RexNode node) {
    int numberOfInputRefs = 0;
    boolean isSupported = true;

    if (node instanceof RexCall) {
      RexCall compositeNode = (RexCall) node;

      // Only support comparisons in a predicate, some sql functions such as:
      //  CAST, TRIM? and REVERSE? should be supported as well.
      if (!node.getKind().belongsTo(SUPPORTED_OPS)) {
        isSupported = false;
      }

      for (RexNode operand : compositeNode.getOperands()) {
        // All operands must be supported for a parent node to be supported.
        Pair<Boolean, Integer> childSupported = isSupported(operand);
        // BigQuery supports complex combinations of both conjunctions (AND) and disjunctions (OR).
        if (!node.getKind().belongsTo(ImmutableSet.of(AND, OR))) {
          // Predicate functions, where more than one field is involved are unsupported.
          numberOfInputRefs += childSupported.getRight();
        }
        isSupported = numberOfInputRefs < 2 && childSupported.getLeft();
      }
    } else if (node instanceof RexInputRef) {
      numberOfInputRefs++;
    } else if (node instanceof RexLiteral) {
      // RexLiterals are expected, but no action is needed.
    } else {
      throw new RuntimeException(
          "Encountered an unexpected node type: " + node.getClass().getSimpleName());
    }

    return Pair.of(isSupported, numberOfInputRefs);
  }
}
