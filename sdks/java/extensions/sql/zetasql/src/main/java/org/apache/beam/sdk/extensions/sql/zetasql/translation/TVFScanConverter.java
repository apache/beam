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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.zetasql.FileDescriptorSetsBuilder;
import com.google.zetasql.FunctionProtos.TableValuedFunctionProto;
import com.google.zetasql.TableValuedFunction.FixedOutputSchemaTVF;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTVFArgument;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTVFScan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;

/** Converts TVFScan. */
class TVFScanConverter extends RelConverter<ResolvedTVFScan> {

  TVFScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public RelNode convert(ResolvedTVFScan zetaNode, List<RelNode> inputs) {
    RelNode input = inputs.get(0);
    RexCall call =
        getExpressionConverter()
            .convertTableValuedFunction(
                input,
                zetaNode.getTvf(),
                zetaNode.getArgumentList(),
                zetaNode.getArgumentList().get(0).getScan() != null
                    ? zetaNode.getArgumentList().get(0).getScan().getColumnList()
                    : Collections.emptyList());
    @SuppressWarnings("argument.type.incompatible") // create function accepts null for elementType
    RelNode tableFunctionScan =
        LogicalTableFunctionScan.create(
            getCluster(), inputs, call, null, call.getType(), Collections.EMPTY_SET);

    // Pure SQL UDF's language body is built bottom up, so FunctionArgumentRefMapping should be
    // already consumed thus it can be cleared now.
    context.clearFunctionArgumentRefMapping();
    return tableFunctionScan;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedTVFScan zetaNode) {
    List<ResolvedNode> inputs = new ArrayList();
    if (zetaNode.getTvf() != null
        && context
            .getUserDefinedTableValuedFunctions()
            .containsKey(zetaNode.getTvf().getNamePath())) {
      inputs.add(checkArgumentNotNull(context.getUserDefinedTableValuedFunctions()
              .get(zetaNode.getTvf().getNamePath())));
    }

    for (ResolvedTVFArgument argument : zetaNode.getArgumentList()) {
      if (argument.getScan() != null) {
        inputs.add(argument.getScan());
      }
    }

    // Extract ResolvedArguments for solving ResolvedArgumentRef in later conversion.
    if (zetaNode.getTvf() instanceof FixedOutputSchemaTVF) {
      FileDescriptorSetsBuilder temp = new FileDescriptorSetsBuilder();
      // TODO: migrate to public Java API to retrieve FunctionSignature.
      TableValuedFunctionProto tableValuedFunctionProto = zetaNode.getTvf().serialize(temp);
      for (int i = 0; i < tableValuedFunctionProto.getSignature().getArgumentList().size(); i++) {
        String argumentName =
            tableValuedFunctionProto.getSignature().getArgument(i).getOptions().getArgumentName();
        if (zetaNode.getArgumentList().get(i).nodeKind() == ResolvedNodeKind.RESOLVED_TVFARGUMENT) {
          ResolvedTVFArgument resolvedTVFArgument = zetaNode.getArgumentList().get(i);
          if (resolvedTVFArgument.getExpr().nodeKind() == ResolvedNodeKind.RESOLVED_LITERAL) {
            ResolvedLiteral literal = (ResolvedLiteral) resolvedTVFArgument.getExpr();
            context.addToFunctionArgumentRefMapping(
                argumentName, getExpressionConverter().convertResolvedLiteral(literal));
          }
        }
      }
    }
    return inputs;
  }
}
