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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedProjectScan;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;

/** Converts projection. */
class ProjectScanConverter extends RelConverter<ResolvedProjectScan> {

  ProjectScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedProjectScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedProjectScan zetaNode, List<RelNode> inputs) {
    RelNode input = inputs.get(0);

    List<RexNode> projects =
        getExpressionConverter().retrieveRexNode(zetaNode, input.getRowType().getFieldList());
    List<String> fieldNames = getTrait().retrieveFieldNames(zetaNode.getColumnList());
    return LogicalProject.create(input, ImmutableList.of(), projects, fieldNames);
  }
}
