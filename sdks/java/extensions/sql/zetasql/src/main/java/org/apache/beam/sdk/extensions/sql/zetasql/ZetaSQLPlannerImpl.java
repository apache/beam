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

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ConversionContext;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ExpressionConverter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.QueryStatementConverter;
import org.apache.beam.vendor.calcite.v1_28_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexExecutor;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Program;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Util;

/** ZetaSQLPlannerImpl. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ZetaSQLPlannerImpl {
  private final SchemaPlus defaultSchemaPlus;

  // variables that are used in Calcite's planner.
  private final FrameworkConfig config;
  private RelOptPlanner planner;
  private JavaTypeFactory typeFactory;
  private final RexExecutor executor;
  private final ImmutableList<Program> programs;

  private String defaultTimezone = "UTC"; // choose UTC (offset 00:00) unless explicitly set

  ZetaSQLPlannerImpl(FrameworkConfig config) {
    this.config = config;
    this.executor = config.getExecutor();
    this.programs = config.getPrograms();

    Frameworks.withPlanner(
        (cluster, relOptSchema, rootSchema) -> {
          Util.discard(rootSchema); // use our own defaultSchema
          typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
          planner = cluster.getPlanner();
          planner.setExecutor(executor);
          return null;
        },
        config);

    this.defaultSchemaPlus = config.getDefaultSchema();
  }

  public RelRoot rel(String sql, QueryParameters params) {
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    AnalyzerOptions options = SqlAnalyzer.getAnalyzerOptions(params, defaultTimezone);
    BeamZetaSqlCatalog catalog =
        BeamZetaSqlCatalog.create(
            defaultSchemaPlus, (JavaTypeFactory) cluster.getTypeFactory(), options);

    // Set up table providers that need to be pre-registered
    SqlAnalyzer analyzer = new SqlAnalyzer();
    List<List<String>> tables = analyzer.extractTableNames(sql, options);
    TableResolution.registerTables(this.defaultSchemaPlus, tables);
    QueryTrait trait = new QueryTrait();
    catalog.addTables(tables, trait);

    ResolvedQueryStmt statement = analyzer.analyzeQuery(sql, options, catalog);

    ExpressionConverter expressionConverter =
        new ExpressionConverter(cluster, params, catalog.getUserFunctionDefinitions());
    ConversionContext context = ConversionContext.of(config, expressionConverter, cluster, trait);

    RelNode convertedNode = QueryStatementConverter.convertRootQuery(context, statement);
    return RelRoot.of(convertedNode, SqlKind.ALL);
  }

  RelNode transform(int i, RelTraitSet relTraitSet, RelNode relNode) {
    Program program = programs.get(i);
    return program.run(planner, relNode, relTraitSet, ImmutableList.of(), ImmutableList.of());
  }

  String getDefaultTimezone() {
    return defaultTimezone;
  }

  void setDefaultTimezone(String timezone) {
    defaultTimezone = timezone;
  }

  static LanguageOptions getLanguageOptions() {
    return SqlAnalyzer.baseAnalyzerOptions().getLanguageOptions();
  }
}
