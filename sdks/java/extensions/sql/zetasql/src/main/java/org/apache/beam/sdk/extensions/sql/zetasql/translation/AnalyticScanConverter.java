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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWindowFrameExpr;
import com.google.zetasql.resolvedast.ResolvedWindowFrameExprEnums.BoundaryType;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionGroup;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils;
import org.apache.beam.vendor.bytebuddy.v1_11_0.net.bytebuddy.asm.Advice;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.Window;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.*;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlAggFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

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
        // Transforms input
         LogicalProject input = convert(zetaNode, inputs.get(0));

        // Creates Window Groups from Analytic Function Group
        List<Window.Group> windowGroups = new ArrayList<>();

        for(ResolvedAnalyticFunctionGroup analyticFunctionGroup: zetaNode.getFunctionGroupList()) {
           Window.Group windowGroup = convert(analyticFunctionGroup, input);
           windowGroups.add(windowGroup);
        }

////        return new LogicalWindow(
//                getCluster(),
//                input.getTraitSet(),
//                input,
//                ImmutableList.of(),
//                input.getRowType(),
//                windowGroups
//        );

        return LogicalWindow.create(
                input.getTraitSet(),
                input,
                ImmutableList.of(),
                input.getRowType(),
                windowGroups
        );
    }

    private LogicalProject convert(ResolvedAnalyticScan zetaNode, RelNode input){
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();




        for(ResolvedAnalyticFunctionGroup analyticFunctionGroup: zetaNode.getFunctionGroupList()) {
//            if(analyticFunctionGroup.getPartitionBy() != null) {
//                for (ResolvedColumnRef resolvedColumnRef : analyticFunctionGroup.getPartitionBy().getPartitionByList()) {
//                    projects.add(
//                            getExpressionConverter()
//                                    .convertRexNodeFromResolvedExpr(resolvedColumnRef)
//                    );
////                    fieldNames.add(getTrait().resolveAlias(resolvedColumnRef.getColumn()));
//                }
//            }
//
//            if(analyticFunctionGroup.getOrderBy() != null) {
//                for(ResolvedOrderByItem resolvedOrderByItem: analyticFunctionGroup.getOrderBy().getOrderByItemList()) {
//                    projects.add(
//                            getExpressionConverter()
//                                    .convertRexNodeFromResolvedExpr(resolvedOrderByItem.getColumnRef())
//                    );
////                    fieldNames.add(getTrait().resolveAlias(resolvedOrderByItem.getColumnRef().getColumn()));
//                }
//            }

            for(ResolvedComputedColumn resolvedComputedColumn: analyticFunctionGroup.getAnalyticFunctionList()){
                ResolvedAnalyticFunctionCall resolvedAnalyticFunctionCall = (ResolvedAnalyticFunctionCall) resolvedComputedColumn.getExpr();
                //TODO: handle analytic function calls with more arguments
                ResolvedExpr resolvedExpr = resolvedAnalyticFunctionCall.getArgumentList().get(0);

                projects.add(
                        getExpressionConverter()
                                .convertRexNodeFromResolvedExpr(
                                        resolvedExpr,
                                        zetaNode.getInputScan().getColumnList(),
                                        input.getRowType().getFieldList(),
                                        ImmutableMap.of()));
                fieldNames.add(getTrait().resolveAlias(resolvedComputedColumn.getColumn()));
            }
        }


        return LogicalProject.create(
                input, ImmutableList.of(), projects, fieldNames
        );
    }

    private Window.Group convert(ResolvedAnalyticFunctionGroup functionGroup, RelNode input){

        List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
        ImmutableBitSet keys = ImmutableBitSet.of();
        RexWindowBound upperBound = null;
        RexWindowBound lowerBound = null;
        int ordinal = 0;

       for(ResolvedComputedColumn resolvedComputedColumn: functionGroup.getAnalyticFunctionList()) {
           List<RexNode> operands = new ArrayList<>();
           ResolvedAnalyticFunctionCall analyticFunctionCall = (ResolvedAnalyticFunctionCall) resolvedComputedColumn.getExpr();

           if(upperBound == null && lowerBound == null) {
               lowerBound = convert(analyticFunctionCall.getWindowFrame().getStartExpr());
               upperBound = convert(analyticFunctionCall.getWindowFrame().getEndExpr());
           }

           //TODO: Go through the aggregate conversion and complete it
           //TODO: Make this code and the one in AggregateScanConverter reusable if needed
           SqlAggFunction sqlAggFunction = (SqlAggFunction) SqlOperatorMappingTable.create(analyticFunctionCall);
           RelDataType type = ZetaSqlCalciteTranslationUtils.toCalciteType(
                   analyticFunctionCall
                           .getFunction()
                           .getSignatureList()
                           .get(ordinal)
                           .getResultType()
                           .getType(),
                   false,
                   getCluster().getRexBuilder()
           );

           //TODO: Fill the list of operands of the RexWinAggCall
           RexNode operand = getExpressionConverter().convertRexNodeFromResolvedExpr(analyticFunctionCall.getArgumentList().get(0));
           operands.add(operand);

           Window.RexWinAggCall rexWinAggCall = new Window.RexWinAggCall(
                   sqlAggFunction,
                   type,
                   operands,
//                   ImmutableList.of(), //Add operands (columns used in the function)
                   ordinal,
                   false,
                   false
           );

           aggCalls.add(rexWinAggCall);
           ordinal++;
       }

        return new Window.Group(
                keys, //TODO: add grouping keys
                true,
                lowerBound,
                upperBound,
                RelCollations.EMPTY, //TODO: add order keys
                aggCalls
        );

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

