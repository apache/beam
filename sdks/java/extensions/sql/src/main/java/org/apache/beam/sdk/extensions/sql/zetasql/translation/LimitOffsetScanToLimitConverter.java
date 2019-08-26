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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLimitOffsetScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;

/** Converts LIMIT without ORDER BY. */
class LimitOffsetScanToLimitConverter extends RelConverter<ResolvedLimitOffsetScan> {

  LimitOffsetScanToLimitConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public boolean canConvert(ResolvedLimitOffsetScan zetaNode) {
    return !(zetaNode.getInputScan() instanceof ResolvedOrderByScan);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedLimitOffsetScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedLimitOffsetScan zetaNode, List<RelNode> inputs) {
    RelNode input = inputs.get(0);
    RelCollation relCollation = RelCollations.of(ImmutableList.of());
    RexNode offset =
        zetaNode.getOffset() == null
            ? null
            : getExpressionConverter().convertRexNodeFromResolvedExpr(zetaNode.getOffset());
    RexNode fetch =
        getExpressionConverter()
            .convertRexNodeFromResolvedExpr(
                zetaNode.getLimit(), zetaNode.getColumnList(), input.getRowType().getFieldList());
    return LogicalSort.create(input, relCollation, offset, fetch);
  }
}
