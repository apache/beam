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
package org.apache.beam.sdk.extensions.sql.meta;

import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;

/** This interface defines Beam SQL Table Filter. */
public interface BeamSqlTableFilter {
  /**
   * Identify parts of a predicate that are not supported by the IO push-down capabilities to be
   * preserved in a {@code Calc} following {@code BeamIOSourceRel}.
   *
   * @return {@code List<RexNode>} unsupported by the IO API. Should be empty when an entire
   *     condition is supported, or an unchanged {@code List<RexNode>} when predicate push-down is
   *     not supported at all.
   */
  List<RexNode> getNotSupported();

  /**
   * This is primarily used by the cost based optimization to determine the benefit of performing
   * predicate push-down for an IOSourceRel.
   *
   * @return number of supported filters.
   */
  int numSupported();

  /**
   * Count a number of {@code RexNode}s involved in all supported filters.
   *
   * @param filterNodes {@code List<RexNode>} supported filters.
   * @return number of expressions involved in all supported filters.
   */
  static int expressionsInFilter(List<RexNode> filterNodes) {
    int childSum =
        filterNodes.stream()
            .filter(n -> n instanceof RexCall)
            .mapToInt(n -> expressionsInFilter(((RexCall) n).getOperands()))
            .sum();
    return filterNodes.size() + childSum;
  }
}
