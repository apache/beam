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

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedArrayScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;

/** Converts array scan that represents join of an uncollect(array_field) to uncollect. */
class ArrayScanToJoinConverter extends RelConverter<ResolvedArrayScan> {

  ArrayScanToJoinConverter(ConversionContext context) {
    super(context);
  }

  /** This is the case of {@code table [LEFT|INNER] JOIN UNNEST(table.array_field) on join_expr}. */
  @Override
  public boolean canConvert(ResolvedArrayScan zetaNode) {
    return zetaNode.getInputScan() != null && zetaNode.getJoinExpr() != null;
  }

  /** Left input is converted from input scan. */
  @Override
  public List<ResolvedNode> getInputs(ResolvedArrayScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  /** Returns a LogicJoin. */
  @Override
  public RelNode convert(ResolvedArrayScan zetaNode, List<RelNode> inputs) {
    List<RexNode> projects = new ArrayList<>();

    RelNode leftInput = inputs.get(0);

    ResolvedColumnRef columnRef = (ResolvedColumnRef) zetaNode.getArrayExpr();
    CorrelationId correlationId = getCluster().createCorrel();
    getCluster().getQuery().mapCorrel(correlationId.getName(), leftInput);
    String columnName =
        String.format(
            "%s%s",
            zetaNode.getElementColumn().getTableName(), zetaNode.getElementColumn().getName());

    projects.add(
        getCluster()
            .getRexBuilder()
            .makeFieldAccess(
                getCluster().getRexBuilder().makeCorrel(leftInput.getRowType(), correlationId),
                getExpressionConverter()
                    .indexOfProjectionColumnRef(
                        columnRef.getColumn().getId(), zetaNode.getInputScan().getColumnList())));

    RelNode projectNode =
        LogicalProject.create(
            LogicalValues.createOneRow(getCluster()), projects, ImmutableList.of(columnName));

    // Create an UnCollect
    // TODO: how to handle ordinality.
    Uncollect uncollectNode = Uncollect.create(projectNode.getTraitSet(), projectNode, false);
    // The InputRef should only be 0 because Uncollect has only one field.
    RelNode rightInput =
        LogicalProject.create(
            uncollectNode,
            ImmutableList.of(getCluster().getRexBuilder().makeInputRef(uncollectNode, 0)),
            ImmutableList.of(columnName));

    // Join condition should be a RexNode converted from join_expr.
    RexNode condition =
        getExpressionConverter().convertRexNodeFromResolvedExpr(zetaNode.getJoinExpr());
    JoinRelType joinRelType = zetaNode.getIsOuter() ? JoinRelType.LEFT : JoinRelType.INNER;

    return LogicalJoin.create(leftInput, rightInput, condition, ImmutableSet.of(), joinRelType);
  }
}
