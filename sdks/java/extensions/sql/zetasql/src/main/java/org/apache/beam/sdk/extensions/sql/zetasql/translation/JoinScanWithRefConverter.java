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

import static org.apache.beam.sdk.extensions.sql.zetasql.translation.JoinScanConverter.convertResolvedJoinType;

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedJoinScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWithRefScan;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Converts joins where at least one of the inputs is a WITH subquery. */
class JoinScanWithRefConverter extends RelConverter<ResolvedJoinScan> {

  JoinScanWithRefConverter(ConversionContext context) {
    super(context);
  }

  /** This is a special logic due to re-indexed column reference in WithScan. */
  @Override
  public boolean canConvert(ResolvedJoinScan zetaNode) {
    return zetaNode.getLeftScan() instanceof ResolvedWithRefScan
        || zetaNode.getRightScan() instanceof ResolvedWithRefScan;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedJoinScan zetaNode) {
    return ImmutableList.of(zetaNode.getLeftScan(), zetaNode.getRightScan());
  }

  @Override
  public RelNode convert(ResolvedJoinScan zetaNode, List<RelNode> inputs) {
    RelNode calciteLeftInput = inputs.get(0);
    RelNode calciteRightInput = inputs.get(1);

    List<ResolvedColumn> zetaLeftColumnList = getColumnsForScan(zetaNode.getLeftScan());
    List<ResolvedColumn> zetaRightColumnList = getColumnsForScan(zetaNode.getRightScan());

    final RexNode condition;
    if (zetaNode.getJoinExpr() == null) {
      condition = getExpressionConverter().trueLiteral();
    } else {
      condition =
          getExpressionConverter()
              .convertRexNodeFromResolvedExprWithRefScan(
                  zetaNode.getJoinExpr(),
                  zetaNode.getLeftScan().getColumnList(),
                  calciteLeftInput.getRowType().getFieldList(),
                  zetaLeftColumnList,
                  zetaNode.getRightScan().getColumnList(),
                  calciteRightInput.getRowType().getFieldList(),
                  zetaRightColumnList);
    }

    return LogicalJoin.create(
        calciteLeftInput,
        calciteRightInput,
        condition,
        ImmutableSet.of(),
        convertResolvedJoinType(zetaNode.getJoinType()));
  }

  /**
   * WithRefScan doesn't have columns in it, it only references a WITH query by name, we have to
   * look up the actual query node in the context by that name.
   *
   * <p>The context has a map of WITH queries populated when the inputs to this JOIN are parsed.
   */
  // TODO: Fix Later
  @SuppressWarnings("nullness")
  private List<ResolvedColumn> getColumnsForScan(ResolvedScan resolvedScan) {
    return resolvedScan instanceof ResolvedWithRefScan
        ? getTrait()
            .withEntries
            .get(((ResolvedWithRefScan) resolvedScan).getWithQueryName())
            .getWithSubquery()
            .getColumnList()
        : resolvedScan.getColumnList();
  }
}
