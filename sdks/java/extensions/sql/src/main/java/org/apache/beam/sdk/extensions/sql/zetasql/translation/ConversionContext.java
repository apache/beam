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

import org.apache.beam.sdk.extensions.sql.zetasql.QueryTrait;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;

/** Conversion context, some rules need this data to convert the nodes. */
public class ConversionContext {
  private final FrameworkConfig config;
  private final ExpressionConverter expressionConverter;
  private final RelOptCluster cluster;
  private final QueryTrait trait;

  public static ConversionContext of(
      FrameworkConfig config,
      ExpressionConverter expressionConverter,
      RelOptCluster cluster,
      QueryTrait trait) {
    return new ConversionContext(config, expressionConverter, cluster, trait);
  }

  private ConversionContext(
      FrameworkConfig config,
      ExpressionConverter expressionConverter,
      RelOptCluster cluster,
      QueryTrait trait) {
    this.config = config;
    this.expressionConverter = expressionConverter;
    this.cluster = cluster;
    this.trait = trait;
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
}
