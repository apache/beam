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
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Converts array scan that represents a reference to an array column, or an (possibly nested) array
 * field of an struct column to uncollect.
 */
class ArrayScanColumnRefToUncollect extends RelConverter<ResolvedNodes.ResolvedArrayScan> {
  ArrayScanColumnRefToUncollect(ConversionContext context) {
    super(context);
  }

  @Override
  public boolean canConvert(ResolvedNodes.ResolvedArrayScan zetaNode) {
    return zetaNode.getInputScan() != null
        && getColumnRef(zetaNode.getArrayExpr()) != null
        && zetaNode.getJoinExpr() == null;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedNodes.ResolvedArrayScan zetaNode) {
    return ImmutableList.of(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedNodes.ResolvedArrayScan zetaNode, List<RelNode> inputs) {
    assert inputs.size() == 1;
    RelNode input = inputs.get(0);
    RexInputRef columnRef =
        (RexInputRef)
            getExpressionConverter()
                .convertRexNodeFromResolvedExpr(
                    getColumnRef(zetaNode.getArrayExpr()),
                    zetaNode.getInputScan().getColumnList(),
                    input.getRowType().getFieldList(),
                    ImmutableMap.of());

    CorrelationId correlationId = new CorrelationId(0);
    RexNode convertedColumnRef =
        getCluster()
            .getRexBuilder()
            .makeFieldAccess(
                getCluster().getRexBuilder().makeCorrel(input.getRowType(), correlationId),
                columnRef.getIndex());

    String fieldName =
        String.format(
            "%s%s",
            zetaNode.getElementColumn().getTableName(), zetaNode.getElementColumn().getName());

    RelNode projectNode =
        LogicalProject.create(
            createOneRow(getCluster()),
            Collections.singletonList(
                convertArrayExpr(
                    zetaNode.getArrayExpr(), getCluster().getRexBuilder(), convertedColumnRef)),
            ImmutableList.of(fieldName));

    boolean ordinality = (zetaNode.getArrayOffsetColumn() != null);
    RelNode uncollect = Uncollect.create(projectNode.getTraitSet(), projectNode, ordinality);

    return LogicalCorrelate.create(
        input,
        uncollect,
        correlationId,
        ImmutableBitSet.of(columnRef.getIndex()),
        JoinRelType.INNER);
  }

  private static ResolvedNodes.ResolvedColumnRef getColumnRef(ResolvedNode arrayExpr) {
    while (arrayExpr instanceof ResolvedNodes.ResolvedGetStructField) {
      arrayExpr = ((ResolvedNodes.ResolvedGetStructField) arrayExpr).getExpr();
    }
    return arrayExpr instanceof ResolvedNodes.ResolvedColumnRef
        ? (ResolvedNodes.ResolvedColumnRef) arrayExpr
        : null;
  }

  private static RexNode convertArrayExpr(
      ResolvedNodes.ResolvedExpr expr, RexBuilder builder, RexNode convertedColumnRef) {
    if (expr instanceof ResolvedNodes.ResolvedColumnRef) {
      return convertedColumnRef;
    }
    ResolvedNodes.ResolvedGetStructField getStructField =
        (ResolvedNodes.ResolvedGetStructField) expr;
    return builder.makeFieldAccess(
        convertArrayExpr(getStructField.getExpr(), builder, convertedColumnRef),
        (int) getStructField.getFieldIdx());
  }
}
