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

import com.google.zetasql.resolvedast.*;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWindowFrameExpr;
import com.google.zetasql.resolvedast.ResolvedWindowFrameExprEnums.BoundaryType;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionGroup;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlAggFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("UnusedVariable")
public class AnalyticScanConverter extends RelConverter<ResolvedAnalyticScan> {

    AnalyticScanConverter(ConversionContext context) {
        super(context);
    }

    @Override
    public List<ResolvedNode> getInputs(ResolvedAnalyticScan zetaNode) {
        return Collections.singletonList(zetaNode.getInputScan());
    }

    @Override
    public RelNode convert(ResolvedAnalyticScan zetaNode, List<RelNode> inputs) {
        RelNode input = inputs.get(0);
        ImmutableList<RexNode> projects = getProjects(zetaNode);
        ImmutableList<String> fieldNames = getFieldNames(zetaNode);

        return LogicalProject.create(
                input,
                ImmutableList.of(),
                projects,
                fieldNames);
    }

    private ImmutableList<RexNode> getProjects(ResolvedAnalyticScan zetaNode){
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>();
        int index = 0;
        for(ResolvedColumn resolvedColumn: zetaNode.getInputScan().getColumnList()) {
            RelDataType type = ZetaSqlCalciteTranslationUtils.toCalciteType(resolvedColumn.getType(), false, rexBuilder);
            RexInputRef columnRef = new RexInputRef(index, type);

            projects.add(columnRef);
            index = index+1;
        }

        for(ResolvedAnalyticFunctionGroup group : zetaNode.getFunctionGroupList()){
            List<RexNode> partitionKeys = new ArrayList<>();
            List<RexFieldCollation> orderKeys = new ArrayList<>();

            if(group.getPartitionBy() != null) {
                for(ResolvedExpr expr: group.getPartitionBy().getPartitionByList()) {
                    partitionKeys.add(getExpressionConverter().convertRexNodeFromResolvedExpr(expr));
                }
            }

            if(group.getOrderBy() != null) {
                for(ResolvedOrderByItem item: group.getOrderBy().getOrderByItemList()) {
                    RexFieldCollation collation = new RexFieldCollation(
                            getExpressionConverter().convertRexNodeFromResolvedExpr(item.getColumnRef()),
                            ImmutableSet.of());
                    orderKeys.add(collation);
                }
            }

            for(ResolvedComputedColumn computedColumn: group.getAnalyticFunctionList()) {
               ResolvedAnalyticFunctionCall analyticFunction = (ResolvedAnalyticFunctionCall) computedColumn.getExpr();
               List<RexNode> operands = new ArrayList<>();
               boolean isRows = (analyticFunction.getWindowFrame().getFrameUnit() == ResolvedWindowFrameEnums.FrameUnit.ROWS);

               for(ResolvedExpr expr: analyticFunction.getArgumentList()) {
                   operands.add(getExpressionConverter().convertRexNodeFromResolvedExpr(expr));
               }

               SqlAggFunction sqlAggFunction = (SqlAggFunction) SqlOperatorMappingTable.create(analyticFunction);
               RelDataType type = ZetaSqlCalciteTranslationUtils.toCalciteType(
                       analyticFunction
                               .getFunction()
                               .getSignatureList()
                               .get(0)
                               .getResultType()
                               .getType(),
                       false,
                       rexBuilder
               );

               projects.add(rexBuilder.makeOver(
                       type,
                       sqlAggFunction,
                       operands,
                       ImmutableList.copyOf(partitionKeys),
                       ImmutableList.copyOf(orderKeys),
                       convert(analyticFunction.getWindowFrame().getStartExpr()),
                       convert(analyticFunction.getWindowFrame().getEndExpr()),
                       isRows, true, false, false, false
               ));

           }

        }
        return ImmutableList.copyOf(projects);
    }

    private ImmutableList<String> getFieldNames(ResolvedAnalyticScan zetaNode) {
        List<String> fieldNames = new ArrayList<>();

        for(ResolvedColumn column: zetaNode.getInputScan().getColumnList()) {
            fieldNames.add(getTrait().resolveAlias(column));
        }

        for(ResolvedAnalyticFunctionGroup functionGroup: zetaNode.getFunctionGroupList())  {
            for(ResolvedComputedColumn computedColumn: functionGroup.getAnalyticFunctionList()) {
                fieldNames.add(getTrait().resolveAlias(computedColumn.getColumn()));
            }
        }

        return ImmutableList.copyOf(fieldNames);
    }

    private RexWindowBound convert(ResolvedWindowFrameExpr frameExpr) {
        BoundaryType boundaryType = frameExpr.getBoundaryType();

        if(boundaryType == BoundaryType.UNBOUNDED_PRECEDING) {
           return RexWindowBounds.UNBOUNDED_PRECEDING;
        }
        else if(boundaryType == BoundaryType.UNBOUNDED_FOLLOWING) {
            return RexWindowBounds.UNBOUNDED_FOLLOWING;
        }
        else if(boundaryType == BoundaryType.CURRENT_ROW) {
            return RexWindowBounds.CURRENT_ROW;
        }
        else if(boundaryType == BoundaryType.OFFSET_FOLLOWING) {
            RexNode offset = getExpressionConverter().convertResolvedLiteral((ResolvedLiteral) frameExpr.getExpression());
            return RexWindowBounds.following(offset);
        }
        else if(boundaryType == BoundaryType.OFFSET_PRECEDING) {
            RexNode offset = getExpressionConverter().convertResolvedLiteral((ResolvedLiteral) frameExpr.getExpression());
            return RexWindowBounds.preceding(offset);
        }
        else{
            throw new UnsupportedOperationException(
                    String.format("Conversion of %s is not supported", boundaryType)
            );
        }

    }


}

