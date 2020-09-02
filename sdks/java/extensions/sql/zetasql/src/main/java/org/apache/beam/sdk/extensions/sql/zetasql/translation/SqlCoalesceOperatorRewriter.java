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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Rewrites COALESCE calls as CASE ($case_no_value) calls.
 *
 * <p>Turns <code>COALESCE(a, b, c)</code> into:
 *
 * <pre><code>CASE
 *   WHEN a IS NOT NULL THEN a
 *   WHEN b IS NOT NULL THEN b
 *   ELSE c
 *   END</code></pre>
 *
 * <p>There is also a special case for the single-argument case: <code>COALESCE(a)</code> becomes
 * just <code>a</code>.
 */
class SqlCoalesceOperatorRewriter implements SqlOperatorRewriter {
  @Override
  public RexNode apply(RexBuilder rexBuilder, List<RexNode> operands) {
    Preconditions.checkArgument(
        operands.size() >= 1, "COALESCE should have at least one argument in function call.");

    // No need for a case operator if there's only one operand
    if (operands.size() == 1) {
      return operands.get(0);
    }

    SqlOperator op = SqlStdOperatorTable.CASE;

    List<RexNode> newOperands = new ArrayList<>();
    for (RexNode operand : Util.skipLast(operands)) {
      newOperands.add(
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ImmutableList.of(operand)));
      newOperands.add(operand);
    }
    newOperands.add(Util.last(operands));

    return rexBuilder.makeCall(op, newOperands);
  }
}
