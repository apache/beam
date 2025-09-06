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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.io.iceberg.FilterUtils.SUPPORTED_OPS;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind.OR;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IcebergFilter implements BeamSqlTableFilter {
  private @Nullable List<RexNode> supported;
  private @Nullable List<RexNode> unsupported;
  private final List<RexNode> predicateCNF;

  public IcebergFilter(List<RexNode> predicateCNF) {
    this.predicateCNF = predicateCNF;
  }

  private void maybeInitialize() {
    if (supported != null && unsupported != null) {
      return;
    }
    ImmutableList.Builder<RexNode> supportedBuilder = ImmutableList.builder();
    ImmutableList.Builder<RexNode> unsupportedBuilder = ImmutableList.builder();
    for (RexNode node : predicateCNF) {
      if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        throw new IllegalArgumentException(
            "Predicate node '"
                + node.getClass().getSimpleName()
                + "' should be a boolean expression, but was: "
                + node.getType().getSqlTypeName());
      }

      if (isSupported(node).getLeft()) {
        supportedBuilder.add(node);
      } else {
        unsupportedBuilder.add(node);
      }
    }
    supported = supportedBuilder.build();
    unsupported = unsupportedBuilder.build();
  }

  @Override
  public List<RexNode> getNotSupported() {
    maybeInitialize();
    return checkStateNotNull(unsupported);
  }

  @Override
  public int numSupported() {
    maybeInitialize();
    return BeamSqlTableFilter.expressionsInFilter(checkStateNotNull(supported));
  }

  public List<RexNode> getSupported() {
    maybeInitialize();
    return checkStateNotNull(supported);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(IcebergFilter.class)
        .add(
            "supported",
            checkStateNotNull(supported).stream()
                .map(RexNode::toString)
                .collect(Collectors.joining()))
        .add(
            "unsupported",
            checkStateNotNull(unsupported).stream()
                .map(RexNode::toString)
                .collect(Collectors.joining()))
        .toString();
  }

  /**
   * Check whether a {@code RexNode} is supported. As of right now Iceberg supports: 1. Complex
   * predicates (both conjunction and disjunction). 2. Comparison between a column and a literal.
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
      if (!SUPPORTED_OPS.contains(node.getKind())) {
        isSupported = false;
      } else {
        for (RexNode operand : compositeNode.getOperands()) {
          // All operands must be supported for a parent node to be supported.
          Pair<Boolean, Integer> childSupported = isSupported(operand);
          if (!node.getKind().belongsTo(ImmutableSet.of(AND, OR))) {
            numberOfInputRefs += childSupported.getRight();
          }
          // Predicate functions with multiple columns are unsupported.
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
