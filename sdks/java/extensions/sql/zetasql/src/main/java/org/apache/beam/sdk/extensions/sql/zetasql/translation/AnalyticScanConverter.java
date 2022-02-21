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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionGroup;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils;
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
           Window.Group windowGroup = convert(analyticFunctionGroup);
           windowGroups.add(windowGroup);
        }

        return new LogicalWindow(
                getCluster(),
                input.getTraitSet(),
                input,
                ImmutableList.of(),
                inputs.get(0).getRowType(),
                windowGroups
        );

    }

    private LogicalProject convert(ResolvedAnalyticScan zetaNode, RelNode input){
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();


        for(ResolvedAnalyticFunctionGroup analyticFunctionGroup: zetaNode.getFunctionGroupList()) {
            for(ResolvedComputedColumn resolvedComputedColumn: analyticFunctionGroup.getAnalyticFunctionList()){
                ResolvedAnalyticFunctionCall resolvedAnalyticFunctionCall = (ResolvedAnalyticFunctionCall) resolvedComputedColumn.getExpr();
                ResolvedNodes.ResolvedExpr resolvedExpr = resolvedAnalyticFunctionCall.getArgumentList().get(0);

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

    private Window.Group convert(ResolvedAnalyticFunctionGroup functionGroup){

        List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
        ImmutableBitSet keys = ImmutableBitSet.of();
        RexWindowBound upperBound = null;
        RexWindowBound lowerBound = null;
        int ordinal = 0;

       //Iterates over AnalyticFunctionList
       for(ResolvedComputedColumn resolvedComputedColumn: functionGroup.getAnalyticFunctionList()) {
           ResolvedAnalyticFunctionCall analyticFunctionCall = (ResolvedAnalyticFunctionCall) resolvedComputedColumn.getExpr();

           // Gets window bounds from an analytic function in the list
           if(upperBound == null && lowerBound == null) {
               lowerBound = RexWindowBounds.UNBOUNDED_PRECEDING;
               upperBound = RexWindowBounds.UNBOUNDED_FOLLOWING;
           }

           //Transforms aggregate call to RexAggWinCalls
           SqlAggFunction sqlAggFunction = (SqlAggFunction) SqlOperatorMappingTable.create(analyticFunctionCall);
           RelDataType type = ZetaSqlCalciteTranslationUtils.toCalciteType(
                   analyticFunctionCall
                           .getFunction()
                           .getSignatureList()
                           .get(0)
                           .getResultType()
                           .getType(),
                   false,
                   getCluster().getRexBuilder()
           );

           Window.RexWinAggCall rexWinAggCall = new Window.RexWinAggCall(
                   sqlAggFunction,
                   type,
                   ImmutableList.of(),
                   ordinal++,
                   false,
                    false
           );

           aggCalls.add(rexWinAggCall);
       }

        return new Window.Group(
                keys,
                true,
                lowerBound,
                upperBound,
                RelCollations.EMPTY,
                aggCalls
        );

    }

//    private RexWindowBound getWindowBound() {
//        return null;
//    }


}

