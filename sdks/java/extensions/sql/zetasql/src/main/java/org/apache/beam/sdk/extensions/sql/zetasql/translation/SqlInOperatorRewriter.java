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

import java.util.List;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Rewrites $in calls as SEARCH calls. */
class SqlInOperatorRewriter implements SqlOperatorRewriter {
  @Override
  public RexNode apply(RexBuilder rexBuilder, List<RexNode> operands) {
    Preconditions.checkArgument(
        operands.size() >= 2, "IN should have at least two arguments in function call.");
    final RexNode arg = operands.get(0);
    final List<RexNode> ranges = ImmutableList.copyOf(operands.subList(1, operands.size()));

    // ZetaSQL has weird behavior for NULL...
    for (RexNode node : ranges) {
      if (node instanceof RexLiteral && ((RexLiteral) node).isNull()) {
        throw new UnsupportedOperationException("IN NULL unsupported");
      }
    }

    return rexBuilder.makeIn(arg, ranges);
  }
}
