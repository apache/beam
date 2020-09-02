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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;

/**
 * This default implementation of {@link BeamSqlTableFilter} interface. Assumes that predicate
 * push-down is not supported.
 */
public final class DefaultTableFilter implements BeamSqlTableFilter {
  private final List<RexNode> filters;

  DefaultTableFilter(List<RexNode> filters) {
    this.filters = filters;
  }

  /**
   * Since predicate push-down is assumed not to be supported by default - return an unchanged list
   * of filters to be preserved.
   *
   * @return Predicate {@code List<RexNode>} which are not supported. To make a single RexNode
   *     expression all of the nodes must be joined by a logical AND.
   */
  @Override
  public List<RexNode> getNotSupported() {
    return filters;
  }

  @Override
  public int numSupported() {
    return 0;
  }
}
