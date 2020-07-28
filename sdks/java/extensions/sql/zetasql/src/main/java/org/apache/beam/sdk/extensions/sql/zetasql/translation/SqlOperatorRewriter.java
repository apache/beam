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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;

/** Interface for rewriting calls a specific ZetaSQL operator. */
interface SqlOperatorRewriter {
  /**
   * Create and return a new {@link RexNode} that represents a call to this operator with the
   * specified operands.
   *
   * @param rexBuilder A {@link RexBuilder} instance to use for creating new {@link RexNode}s
   * @param operands The original list of {@link RexNode} operands passed to this operator call
   * @return The created RexNode
   */
  RexNode apply(RexBuilder rexBuilder, List<RexNode> operands);
}
