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
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTVFArgument;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTVFScan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelRecordType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;

class TVFScanConverter extends RelConverter<ResolvedTVFScan> {

  TVFScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public RelNode convert(ResolvedTVFScan zetaNode, List<RelNode> inputs) {
    RelNode input = inputs.get(0);
    RelNode tableFunctionScan =
        LogicalTableFunctionScan.create(
            getCluster(),
            inputs,
            getExpressionConverter()
                .convertTableValuedFunction(
                    input,
                    zetaNode.getTvf(),
                    zetaNode.getArgumentList(),
                    zetaNode.getArgumentList().get(0).getScan().getColumnList()),
            null,
            createRowTypeWithWindowStartAndEnd(input.getRowType()),
            Collections.EMPTY_SET);

    return tableFunctionScan;
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedTVFScan zetaNode) {
    List<ResolvedNode> inputs = new ArrayList();
    for (ResolvedTVFArgument argument : zetaNode.getArgumentList()) {
      if (argument.getScan() != null) {
        inputs.add(argument.getScan());
      }
    }
    return inputs;
  }

  private RelDataType createRowTypeWithWindowStartAndEnd(RelDataType inputRowType) {
    List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
    RelDataType timestampType = getCluster().getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

    RelDataTypeField windowStartField =
        new RelDataTypeFieldImpl("window_start", newFields.size(), timestampType);
    newFields.add(windowStartField);
    RelDataTypeField windowEndField =
        new RelDataTypeFieldImpl("window_end", newFields.size(), timestampType);
    newFields.add(windowEndField);

    return new RelRecordType(inputRowType.getStructKind(), newFields);
  }
}
