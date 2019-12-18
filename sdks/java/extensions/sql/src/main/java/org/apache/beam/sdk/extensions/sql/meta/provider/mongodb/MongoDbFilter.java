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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.OR;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;

public class MongoDbFilter implements BeamSqlTableFilter {
  private List<RexNode> supported;
  private List<RexNode> unsupported;

  public MongoDbFilter(List<RexNode> predicateCNF) {
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
   * Check whether a {@code RexNode} is supported. To keep things simple:<br>
   * 1. Support comparison operations in predicate, which compare a single field to literal values.
   * 2. Support nested Conjunction (AND), Disjunction (OR) as long as child operations are
   * supported.<br>
   * 3. Support boolean fields.
   *
   * @param node A node to check for predicate push-down support.
   * @return A boolean whether an expression is supported.
   */
  private boolean isSupported(RexNode node) {
    if (node instanceof RexCall) {
      RexCall compositeNode = (RexCall) node;

      if (node.getKind().belongsTo(COMPARISON) || node.getKind().equals(SqlKind.NOT)) {
        int fields = 0;
        for (RexNode operand : compositeNode.getOperands()) {
          if (operand instanceof RexInputRef) {
            fields++;
          } else if (operand instanceof RexLiteral) {
            // RexLiterals are expected, but no action is needed.
          } else {
            // Complex predicates are not supported. Ex: `field1+5 == 10`.
            return false;
          }
        }
        // All comparison operations should have exactly one field reference.
        // Ex: `field1 == field2` is not supported.
        // TODO: Can be supported via Filters#where.
        if (fields == 1) {
          return true;
        }
      } else if (node.getKind().equals(AND) || node.getKind().equals(OR)) {
        // Nested ANDs and ORs are supported as long as all operands are supported.
        for (RexNode operand : compositeNode.getOperands()) {
          if (!isSupported(operand)) {
            return false;
          }
        }
        return true;
      }
    } else if (node instanceof RexInputRef) {
      // When field is a boolean.
      return true;
    } else {
      throw new RuntimeException(
          "Encountered an unexpected node type: " + node.getClass().getSimpleName());
    }

    return false;
  }
}
