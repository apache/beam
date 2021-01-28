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

import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_CREATE_FUNCTION_STMT;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_CREATE_TABLE_FUNCTION_STMT;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_QUERY_STMT;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.JavaUdfLoader;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ConversionContext;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ExpressionConverter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.QueryStatementConverter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.UserFunctionDefinitions;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.UserFunctionDefinitions.JavaScalarFunction;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexExecutor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Program;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** ZetaSQLPlannerImpl. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
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
    QueryTrait trait = new QueryTrait();
    SqlAnalyzer analyzer =
        new SqlAnalyzer(trait, defaultSchemaPlus, (JavaTypeFactory) cluster.getTypeFactory());

    AnalyzerOptions options = SqlAnalyzer.getAnalyzerOptions(params, defaultTimezone);

    // Set up table providers that need to be pre-registered
    List<List<String>> tables = analyzer.extractTableNames(sql, options);
    TableResolution.registerTables(this.defaultSchemaPlus, tables);
    SimpleCatalog catalog =
        analyzer.createPopulatedCatalog(defaultSchemaPlus.getName(), options, tables);

    ImmutableMap.Builder<List<String>, ResolvedCreateFunctionStmt> udfBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<List<String>, ResolvedNode> udtvfBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<List<String>, JavaScalarFunction> javaScalarFunctionBuilder =
        ImmutableMap.builder();
    JavaUdfLoader javaUdfLoader = new JavaUdfLoader();

    ResolvedStatement statement;
    ParseResumeLocation parseResumeLocation = new ParseResumeLocation(sql);
    do {
      statement = analyzer.analyzeNextStatement(parseResumeLocation, options, catalog);
      if (statement.nodeKind() == RESOLVED_CREATE_FUNCTION_STMT) {
        ResolvedCreateFunctionStmt createFunctionStmt = (ResolvedCreateFunctionStmt) statement;
        String functionGroup = SqlAnalyzer.getFunctionGroup(createFunctionStmt);
        if (SqlAnalyzer.USER_DEFINED_FUNCTIONS.equals(functionGroup)) {
          udfBuilder.put(createFunctionStmt.getNamePath(), createFunctionStmt);
        } else if (SqlAnalyzer.USER_DEFINED_JAVA_SCALAR_FUNCTIONS.equals(functionGroup)) {
          String jarPath = getJarPath(createFunctionStmt);
          ScalarFn scalarFn =
              javaUdfLoader.loadScalarFunction(createFunctionStmt.getNamePath(), jarPath);
          javaScalarFunctionBuilder.put(
              createFunctionStmt.getNamePath(),
              UserFunctionDefinitions.JavaScalarFunction.create(scalarFn, jarPath));
        } else {
          throw new IllegalArgumentException(
              String.format("Encountered unrecognized function group %s.", functionGroup));
        }
      } else if (statement.nodeKind() == RESOLVED_CREATE_TABLE_FUNCTION_STMT) {
        ResolvedCreateTableFunctionStmt createTableFunctionStmt =
            (ResolvedCreateTableFunctionStmt) statement;
        udtvfBuilder.put(createTableFunctionStmt.getNamePath(), createTableFunctionStmt.getQuery());
      } else if (statement.nodeKind() == RESOLVED_QUERY_STMT) {
        if (!SqlAnalyzer.isEndOfInput(parseResumeLocation)) {
          throw new UnsupportedOperationException(
              "No additional statements are allowed after a SELECT statement.");
        }
        break;
      }
    } while (!SqlAnalyzer.isEndOfInput(parseResumeLocation));

    if (!(statement instanceof ResolvedQueryStmt)) {
      throw new UnsupportedOperationException(
          "Statement list must end in a SELECT statement, not " + statement.nodeKindString());
    }

    UserFunctionDefinitions userFunctionDefinitions =
        UserFunctionDefinitions.newBuilder()
            .setSqlScalarFunctions(udfBuilder.build())
            .setSqlTableValuedFunctions(udtvfBuilder.build())
            .setJavaScalarFunctions(javaScalarFunctionBuilder.build())
            .build();

    ExpressionConverter expressionConverter =
        new ExpressionConverter(cluster, params, userFunctionDefinitions);
    ConversionContext context = ConversionContext.of(config, expressionConverter, cluster, trait);

    RelNode convertedNode =
        QueryStatementConverter.convertRootQuery(context, (ResolvedQueryStmt) statement);
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

  private static String getJarPath(ResolvedCreateFunctionStmt createFunctionStmt) {
    String jarPath = SqlAnalyzer.getOptionStringValue(createFunctionStmt, "path");
    if (jarPath.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No jar was provided to define function %s. Add 'OPTIONS (path=<jar location>)' to the CREATE FUNCTION statement.",
              String.join(".", createFunctionStmt.getNamePath())));
    }
    return jarPath;
  }
}
