/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.BETWEEN;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.CAST;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.DIVIDE;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.MINUS;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.MOD;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.OR;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.PLUS;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.TIMES;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigQueryFilter implements BeamSqlTableFilter {
  private static final ImmutableSet<SqlKind> SUPPORTED_OPS =
      ImmutableSet.<SqlKind>builder()
          .add(COMPARISON.toArray(new SqlKind[0]))
          // TODO: Check what other functions are supported and add support for them (ex: trim).
          .add(PLUS, MINUS, MOD, DIVIDE, TIMES, LIKE, BETWEEN, CAST, AND, OR)
          .build();
  private List<RexNode> supported;
  private List<RexNode> unsupported;

  public BigQueryFilter(List<RexNode> predicateCNF) {
    supported = new ArrayList<>();
    unsupported = new ArrayList<>();

    for (RexNode node : predicateCNF) {
      if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        throw new IllegalArgumentException(
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

  @Override
  public int numSupported() {
    return BeamSqlTableFilter.expressionsInFilter(supported);
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
   * Check whether a {@code RexNode} is supported. As of right now BigQuery supports: 1. Complex
   * predicates (both conjunction and disjunction). 2. Comparison between a column and a literal.
   *
   * <p>TODO: Check if comparison between two columns is supported. Also over a boolean field.
   *
   * @param node A node to check for predicate push-down support.
   * @return A pair containing a boolean whether an expression is supported and the number of input
   *     references used by the expression.
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
      } else {
        for (RexNode operand : compositeNode.getOperands()) {
          // All operands must be supported for a parent node to be supported.
          Pair<Boolean, Integer> childSupported = isSupported(operand);
          // BigQuery supports complex combinations of both conjunctions (AND) and disjunctions
          // (OR).
          if (!node.getKind().belongsTo(ImmutableSet.of(AND, OR))) {
            numberOfInputRefs += childSupported.getRight();
          }
          // Predicate functions, where more than one field is involved are unsupported.
          isSupported = numberOfInputRefs < 2 && childSupported.getLeft();
        }
      }
    } else if (node instanceof RexInputRef) {
      numberOfInputRefs = 1;
    } else if (node instanceof RexLiteral) {
      // RexLiterals are expected, but no action is needed.
    } else {
      throw new UnsupportedOperationException(
          "Encountered an unexpected node type: " + node.getClass().getSimpleName());
    }

    return Pair.of(isSupported, numberOfInputRefs);
  }
}
