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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind.IN;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
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
   * Check whether a {@code RexNode} is supported. For testing purposes only simple nodes are
   * supported. Ex: comparison between 2 input fields, input field to a literal, literal to a
   * literal.
   *
   * @param node A node to check for predicate push-down support.
   * @return True when a node is supported, false otherwise.
   */
  private boolean isSupported(RexNode node) {
    if (node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
      if (node instanceof RexCall) {
        RexCall compositeNode = (RexCall) node;

        // Only support comparisons in a predicate
        if (!node.getKind().belongsTo(COMPARISON)) {
          return false;
        }

        // Not support IN operator for now
        if (node.getKind().equals(IN)) {
          return false;
        }

        for (RexNode operand : compositeNode.getOperands()) {
          if (!(operand instanceof RexLiteral) && !(operand instanceof RexInputRef)) {
            return false;
          }
        }
      } else if (node instanceof RexInputRef) {
        // When field is a boolean
        return true;
      } else {
        throw new UnsupportedOperationException(
            "Encountered an unexpected node type: " + node.getClass().getSimpleName());
      }
    } else {
      throw new UnsupportedOperationException(
          "Predicate node '"
              + node.getClass().getSimpleName()
              + "' should be a boolean expression, but was: "
              + node.getType().getSqlTypeName());
    }

    return true;
  }
}
