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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Rewrites IFNULL calls as CASE ($case_no_value) calls.
 *
 * <p>Turns <code>IFNULL(expr, null_result)</code> into: <code><pre>CASE
 *   WHEN expr IS NULL THEN null_result
 *   ELSE expr
 *   END</pre></code>
 */
public class SqlIfNullOperatorRewriter implements SqlOperatorRewriter {
  @Override
  public RexNode apply(RexBuilder rexBuilder, List<RexNode> operands) {
    Preconditions.checkArgument(
        operands.size() == 2, "IFNULL should have two arguments in function call.");

    SqlOperator op = SqlStdOperatorTable.CASE;
    List<RexNode> newOperands =
        ImmutableList.of(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ImmutableList.of(operands.get(0))),
            operands.get(1),
            operands.get(0));

    return rexBuilder.makeCall(op, newOperands);
  }
}
