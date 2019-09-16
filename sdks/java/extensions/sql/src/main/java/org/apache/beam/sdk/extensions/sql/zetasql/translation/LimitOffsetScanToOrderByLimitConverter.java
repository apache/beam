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

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLimitOffsetScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByScan;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;

/** Converts ORDER BY LIMIT. */
class LimitOffsetScanToOrderByLimitConverter extends RelConverter<ResolvedLimitOffsetScan> {

  LimitOffsetScanToOrderByLimitConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public boolean canConvert(ResolvedLimitOffsetScan zetaNode) {
    return zetaNode.getInputScan() instanceof ResolvedOrderByScan;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedLimitOffsetScan zetaNode) {
    // The immediate input is the ORDER BY scan which we don't support,
    // but we can handle the ORDER BY LIMIT if we know the underlying projection, for example.
    return Collections.singletonList(
        ((ResolvedOrderByScan) zetaNode.getInputScan()).getInputScan());
  }

  @Override
  public RelNode convert(ResolvedLimitOffsetScan zetaNode, List<RelNode> inputs) {
    ResolvedOrderByScan inputOrderByScan = (ResolvedOrderByScan) zetaNode.getInputScan();
    RelNode input = convertOrderByScanToLogicalScan(inputOrderByScan, inputs.get(0));
    RelCollation relCollation = getRelCollation(inputOrderByScan);

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

  /** Collation is a sort order, as in ORDER BY DESCENDING/ASCENDING. */
  private static RelCollation getRelCollation(ResolvedOrderByScan node) {
    List<RelFieldCollation> fieldCollations =
        node.getOrderByItemList().stream()
            .map(LimitOffsetScanToOrderByLimitConverter::orderByItemToFieldCollation)
            .collect(toList());
    return RelCollationImpl.of(fieldCollations);
  }

  private static RelFieldCollation orderByItemToFieldCollation(ResolvedOrderByItem item) {
    // TODO: might need a column ref mapping here.
    Direction sortDirection = item.getIsDescending() ? DESCENDING : ASCENDING;
    int fieldIndex = (int) item.getColumnRef().getColumn().getId();
    return new RelFieldCollation(fieldIndex, sortDirection);
  }

  private RelNode convertOrderByScanToLogicalScan(ResolvedOrderByScan node, RelNode input) {
    List<RexNode> projects =
        getExpressionConverter()
            .retrieveRexNodeFromOrderByScan(getCluster(), node, input.getRowType().getFieldList());
    List<String> fieldNames = getTrait().retrieveFieldNames(node.getColumnList());

    return LogicalProject.create(input, projects, fieldNames);
  }
}
