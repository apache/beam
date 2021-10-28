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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedJoinScanEnums.JoinType;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedJoinScan;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Converts joins if neither side of the join is a WithRefScan. */
class JoinScanConverter extends RelConverter<ResolvedJoinScan> {

  private static final ImmutableMap<JoinType, JoinRelType> JOIN_TYPES =
      ImmutableMap.of(
          JoinType.INNER,
          JoinRelType.INNER,
          JoinType.FULL,
          JoinRelType.FULL,
          JoinType.LEFT,
          JoinRelType.LEFT,
          JoinType.RIGHT,
          JoinRelType.RIGHT);

  JoinScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public boolean canConvert(ResolvedJoinScan zetaNode) {
    return true;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedJoinScan zetaNode) {
    return ImmutableList.of(zetaNode.getLeftScan(), zetaNode.getRightScan());
  }

  @Override
  public RelNode convert(ResolvedJoinScan zetaNode, List<RelNode> inputs) {
    RelNode convertedLeftInput = inputs.get(0);
    RelNode convertedRightInput = inputs.get(1);

    List<ResolvedColumn> combinedZetaFieldsList =
        ImmutableList.<ResolvedColumn>builder()
            .addAll(zetaNode.getLeftScan().getColumnList())
            .addAll(zetaNode.getRightScan().getColumnList())
            .build();

    List<RelDataTypeField> combinedCalciteFieldsList =
        ImmutableList.<RelDataTypeField>builder()
            .addAll(convertedLeftInput.getRowType().getFieldList())
            .addAll(convertedRightInput.getRowType().getFieldList())
            .build();

    final RexNode condition;
    if (zetaNode.getJoinExpr() == null) {
      condition = getExpressionConverter().trueLiteral();
    } else {
      condition =
          getExpressionConverter()
              .convertRexNodeFromResolvedExpr(
                  zetaNode.getJoinExpr(),
                  combinedZetaFieldsList,
                  combinedCalciteFieldsList,
                  ImmutableMap.of());
    }

    return LogicalJoin.create(
        convertedLeftInput,
        convertedRightInput,
        ImmutableList.of(),
        condition,
        ImmutableSet.of(),
        convertResolvedJoinType(zetaNode.getJoinType()));
  }

  static JoinRelType convertResolvedJoinType(JoinType joinType) {
    if (!JOIN_TYPES.containsKey(joinType)) {
      throw new UnsupportedOperationException("JOIN type: " + joinType + " is unsupported.");
    }

    return JOIN_TYPES.get(joinType);
  }
}
