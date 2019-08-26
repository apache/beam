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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/** Rewrites EXTRACT calls by swapping first two arguments to fit for calcite SqlExtractOperator. */
public class SqlExtractTimestampOperatorRewriter implements SqlOperatorRewriter {
  @Override
  public RexNode apply(RexBuilder rexBuilder, List<RexNode> operands) {
    Preconditions.checkArgument(
        operands.size() == 2,
        "EXTRACT should have two arguments in function call. AT TIME ZONE not supported.");

    SqlOperator op =
        SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get("$extract");

    ImmutableList.Builder<RexNode> newOperands =
        ImmutableList.<RexNode>builder().add(operands.get(1)).add(operands.get(0));

    for (int i = 2; i < operands.size(); ++i) {
      newOperands.add(operands.get(i));
    }

    return rexBuilder.makeCall(op, newOperands.build());
  }
}
