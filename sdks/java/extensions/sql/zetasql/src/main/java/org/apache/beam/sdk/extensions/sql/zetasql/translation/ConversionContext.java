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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.zetasql.QueryTrait;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;

/** Conversion context, some rules need this data to convert the nodes. */
@Internal
public class ConversionContext {
  private final FrameworkConfig config;
  private final ExpressionConverter expressionConverter;
  private final RelOptCluster cluster;
  private final QueryTrait trait;

  // SQL native user-defined table-valued function can be resolved by Analyzer. Keeping the
  // function name to its ResolvedNode mapping so during Plan conversion, UDTVF implementation
  // can replace inputs of TVFScanConverter.
  private final Map<List<String>, ResolvedNode> userDefinedTableValuedFunctions;

  // SQL native user-defined table-valued function can be resolved by Analyzer. Its sql body is
  // converted to ResolvedNode, in which function parameters are replaced with ResolvedArgumentRef.
  // Meanwhile, Analyzer provides values for function parameters because it looks ahead to find
  // the SELECT query. Thus keep the argument name to values (converted to RexNode) mapping in
  // Context for future usage in plan conversion.
  private Map<String, RexNode> functionArgumentRefMapping;

  public static ConversionContext of(
      FrameworkConfig config,
      ExpressionConverter expressionConverter,
      RelOptCluster cluster,
      QueryTrait trait,
      Map<List<String>, ResolvedNode> sqlUDTVF) {
    return new ConversionContext(config, expressionConverter, cluster, trait, sqlUDTVF);
  }

  public static ConversionContext of(
      FrameworkConfig config,
      ExpressionConverter expressionConverter,
      RelOptCluster cluster,
      QueryTrait trait) {
    return new ConversionContext(
        config, expressionConverter, cluster, trait, Collections.emptyMap());
  }

  private ConversionContext(
      FrameworkConfig config,
      ExpressionConverter expressionConverter,
      RelOptCluster cluster,
      QueryTrait trait,
      Map<List<String>, ResolvedNode> sqlUDTVF) {
    this.config = config;
    this.expressionConverter = expressionConverter;
    this.cluster = cluster;
    this.trait = trait;
    this.userDefinedTableValuedFunctions = sqlUDTVF;
    this.functionArgumentRefMapping = new HashMap<>();
  }

  FrameworkConfig getConfig() {
    return config;
  }

  ExpressionConverter getExpressionConverter() {
    return expressionConverter;
  }

  RelOptCluster cluster() {
    return cluster;
  }

  QueryTrait getTrait() {
    return trait;
  }

  Map<List<String>, ResolvedNode> getUserDefinedTableValuedFunctions() {
    return userDefinedTableValuedFunctions;
  }

  Map<String, RexNode> getFunctionArgumentRefMapping() {
    return functionArgumentRefMapping;
  }

  void addToFunctionArgumentRefMapping(String s, RexNode r) {
    getFunctionArgumentRefMapping().put(s, r);
  }

  void clearFunctionArgumentRefMapping() {
    getFunctionArgumentRefMapping().clear();
  }
}
