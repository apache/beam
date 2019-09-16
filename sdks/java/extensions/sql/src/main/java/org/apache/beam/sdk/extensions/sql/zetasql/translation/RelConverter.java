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
import java.util.List;
import org.apache.beam.sdk.extensions.sql.zetasql.QueryTrait;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;

/** A rule that converts Zeta SQL resolved relational node to corresponding Calcite rel node. */
abstract class RelConverter<T extends ResolvedNode> {

  /**
   * Conversion context, contains things like FrameworkConfig, QueryTrait and other state used
   * during conversion.
   */
  protected ConversionContext context;

  RelConverter(ConversionContext context) {
    this.context = context;
  }

  /** Whether this rule can handle the conversion of the specific node. */
  public boolean canConvert(T zetaNode) {
    return true;
  }

  /** Extract Zeta SQL resolved nodes that correspond to the inputs of the current node. */
  public List<ResolvedNode> getInputs(T zetaNode) {
    return ImmutableList.of();
  }

  /**
   * Converts given Zeta SQL node to corresponding Calcite node.
   *
   * <p>{@code inputs} are node inputs that have already been converter to Calcite versions. They
   * correspond to the nodes in {@link #getInputs(ResolvedNode)}.
   */
  public abstract RelNode convert(T zetaNode, List<RelNode> inputs);

  protected RelOptCluster getCluster() {
    return context.cluster();
  }

  protected FrameworkConfig getConfig() {
    return context.getConfig();
  }

  protected ExpressionConverter getExpressionConverter() {
    return context.getExpressionConverter();
  }

  protected QueryTrait getTrait() {
    return context.getTrait();
  }
}
