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

import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedArrayScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Converts array scan that represents an array literal to uncollect. */
class ArrayScanLiteralToUncollectConverter extends RelConverter<ResolvedArrayScan> {

  ArrayScanLiteralToUncollectConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public boolean canConvert(ResolvedArrayScan zetaNode) {
    return zetaNode.getInputScan() == null;
  }

  @Override
  public RelNode convert(ResolvedArrayScan zetaNode, List<RelNode> inputs) {
    RexNode arrayLiteralExpression =
        getExpressionConverter().convertRexNodeFromResolvedExpr(zetaNode.getArrayExpr());

    String fieldName =
        String.format(
            "%s%s",
            zetaNode.getElementColumn().getTableName(), zetaNode.getElementColumn().getName());

    RelNode projectNode =
        LogicalProject.create(
            createOneRow(getCluster()),
            Collections.singletonList(arrayLiteralExpression),
            ImmutableList.of(fieldName));

    boolean ordinality = (zetaNode.getArrayOffsetColumn() != null);

    // These asserts guaranteed by the parser code, but not the data structure.
    // If they aren't true we need to add a Project to reorder columns.
    assert zetaNode.getElementColumn().getId() == 1;
    assert !ordinality || zetaNode.getArrayOffsetColumn().getColumn().getId() == 2;
    return Uncollect.create(projectNode.getTraitSet(), projectNode, ordinality);
  }
}
