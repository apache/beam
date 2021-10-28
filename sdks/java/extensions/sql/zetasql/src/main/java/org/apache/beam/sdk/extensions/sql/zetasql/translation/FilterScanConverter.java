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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFilterScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;

/** Converts filter. */
class FilterScanConverter extends RelConverter<ResolvedFilterScan> {

  FilterScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedFilterScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedFilterScan zetaNode, List<RelNode> inputs) {
    RelNode input = inputs.get(0);
    RexNode condition =
        getExpressionConverter()
            .convertRexNodeFromResolvedExpr(
                zetaNode.getFilterExpr(),
                zetaNode.getInputScan().getColumnList(),
                input.getRowType().getFieldList(),
                context.getFunctionArgumentRefMapping());

    return LogicalFilter.create(input, condition);
  }
}
