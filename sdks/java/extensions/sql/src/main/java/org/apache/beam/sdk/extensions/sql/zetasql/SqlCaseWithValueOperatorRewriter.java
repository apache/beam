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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Rewrites $case_with_value calls as $case_no_value calls.
 *
 * <p>Turns:
 *
 * <pre><code>CASE x
 *   WHEN w1 THEN t1
 *   WHEN w2 THEN t2
 *   ELSE e
 *   END</code></pre>
 *
 * <p>into:
 *
 * <pre><code>CASE
 *   WHEN x == w1 THEN t1
 *   WHEN x == w2 THEN t2
 *   ELSE expr
 *   END</code></pre>
 *
 * <p>Note that the ELSE statement is actually optional, but we don't need to worry about that here
 * because the ZetaSQL analyzer populates the ELSE argument as a NULL literal if it's not specified.
 */
public class SqlCaseWithValueOperatorRewriter implements SqlOperatorRewriter {
  @Override
  public RexNode apply(RexBuilder rexBuilder, List<RexNode> operands) {
    Preconditions.checkArgument(
        operands.size() % 2 == 0 && !operands.isEmpty(),
        "$case_with_value should have an even number of arguments greater than 0 in function call"
            + " (The value operand, the else operand, and paired when/then operands).");
    SqlOperator op = SqlStdOperatorTable.CASE;

    List<RexNode> newOperands = new ArrayList<>();
    RexNode value = operands.get(0);

    for (int i = 1; i < operands.size() - 2; i += 2) {
      RexNode when = operands.get(i);
      RexNode then = operands.get(i + 1);
      newOperands.add(
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ImmutableList.of(value, when)));
      newOperands.add(then);
    }

    RexNode elseOperand = Iterables.getLast(operands);
    newOperands.add(elseOperand);

    return rexBuilder.makeCall(op, newOperands);
  }
}
