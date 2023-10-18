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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLOptions.ErrorMessageMode;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLOptions.ParameterMode;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters.Kind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** Adapter for {@link Analyzer} to simplify the API for parsing the query and resolving the AST. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SqlAnalyzer {
  private static final ImmutableSet<ResolvedNodeKind> SUPPORTED_STATEMENT_KINDS =
      ImmutableSet.of(
          RESOLVED_QUERY_STMT, RESOLVED_CREATE_FUNCTION_STMT, RESOLVED_CREATE_TABLE_FUNCTION_STMT);

  SqlAnalyzer() {}

  /** Returns table names from all statements in the SQL string. */
  List<List<String>> extractTableNames(String sql, AnalyzerOptions options) {
    ParseResumeLocation parseResumeLocation = new ParseResumeLocation(sql);
    ImmutableList.Builder<List<String>> tables = ImmutableList.builder();
    while (!isEndOfInput(parseResumeLocation)) {
      List<List<String>> statementTables =
          Analyzer.extractTableNamesFromNextStatement(parseResumeLocation, options);
      tables.addAll(statementTables);
    }
    return tables.build();
  }

  /**
   * Analyzes the entire SQL code block (which may consist of multiple statements) and returns the
   * resolved query.
   *
   * <p>Assumes there is exactly one SELECT statement in the input, and it must be the last
   * statement in the input.
   */
  ResolvedNodes.ResolvedQueryStmt analyzeQuery(
      String sql, AnalyzerOptions options, BeamZetaSqlCatalog catalog) {
    ParseResumeLocation parseResumeLocation = new ParseResumeLocation(sql);
    ResolvedStatement statement;
    do {
      statement = analyzeNextStatement(parseResumeLocation, options, catalog);
      if (statement.nodeKind() == RESOLVED_QUERY_STMT) {
        if (!SqlAnalyzer.isEndOfInput(parseResumeLocation)) {
          throw new UnsupportedOperationException(
              "No additional statements are allowed after a SELECT statement.");
        }
      }
    } while (!SqlAnalyzer.isEndOfInput(parseResumeLocation));

    if (!(statement instanceof ResolvedNodes.ResolvedQueryStmt)) {
      throw new UnsupportedOperationException(
          "Statement list must end in a SELECT statement, not " + statement.nodeKindString());
    }
    return (ResolvedNodes.ResolvedQueryStmt) statement;
  }

  private static boolean isEndOfInput(ParseResumeLocation parseResumeLocation) {
    return parseResumeLocation.getBytePosition()
        >= parseResumeLocation.getInput().getBytes(UTF_8).length;
  }

  /**
   * Accepts the ParseResumeLocation for the current position in the SQL string. Advances the
   * ParseResumeLocation to the start of the next statement. Adds user-defined functions to the
   * catalog for use in following statements. Returns the resolved AST.
   */
  private ResolvedStatement analyzeNextStatement(
      ParseResumeLocation parseResumeLocation,
      AnalyzerOptions options,
      BeamZetaSqlCatalog catalog) {
    ResolvedStatement resolvedStatement =
        Analyzer.analyzeNextStatement(parseResumeLocation, options, catalog.getZetaSqlCatalog());
    if (resolvedStatement.nodeKind() == RESOLVED_CREATE_FUNCTION_STMT) {
      ResolvedCreateFunctionStmt createFunctionStmt =
          (ResolvedCreateFunctionStmt) resolvedStatement;
      try {
        catalog.addFunction(createFunctionStmt);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(
            String.format(
                "Failed to define function '%s'",
                String.join(".", createFunctionStmt.getNamePath())),
            e);
      }
    } else if (resolvedStatement.nodeKind() == RESOLVED_CREATE_TABLE_FUNCTION_STMT) {
      ResolvedCreateTableFunctionStmt createTableFunctionStmt =
          (ResolvedCreateTableFunctionStmt) resolvedStatement;
      catalog.addTableValuedFunction(createTableFunctionStmt);
    } else if (!SUPPORTED_STATEMENT_KINDS.contains(resolvedStatement.nodeKind())) {
      throw new UnsupportedOperationException(
          "Unrecognized statement type " + resolvedStatement.nodeKindString());
    }
    return resolvedStatement;
  }

  static AnalyzerOptions baseAnalyzerOptions() {
    AnalyzerOptions options = new AnalyzerOptions();
    options.setErrorMessageMode(ErrorMessageMode.ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

    options.getLanguageOptions().setProductMode(ProductMode.PRODUCT_EXTERNAL);
    options
        .getLanguageOptions()
        .setEnabledLanguageFeatures(
            new HashSet<>(
                Arrays.asList(
                    LanguageFeature.FEATURE_CREATE_AGGREGATE_FUNCTION,
                    LanguageFeature.FEATURE_CREATE_TABLE_FUNCTION,
                    LanguageFeature.FEATURE_DISALLOW_GROUP_BY_FLOAT,
                    LanguageFeature.FEATURE_NUMERIC_TYPE,
                    LanguageFeature.FEATURE_TABLE_VALUED_FUNCTIONS,
                    LanguageFeature.FEATURE_TEMPLATE_FUNCTIONS,
                    LanguageFeature.FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE,
                    LanguageFeature.FEATURE_V_1_2_CIVIL_TIME,
                    LanguageFeature.FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS)));
    options.getLanguageOptions().setSupportedStatementKinds(SUPPORTED_STATEMENT_KINDS);

    return options;
  }

  static AnalyzerOptions getAnalyzerOptions(QueryParameters queryParams, String defaultTimezone) {
    AnalyzerOptions options = baseAnalyzerOptions();

    options.setDefaultTimezone(defaultTimezone);

    if (queryParams.getKind() == Kind.NAMED) {
      options.setParameterMode(ParameterMode.PARAMETER_NAMED);
      for (Map.Entry<String, Value> entry : ((Map<String, Value>) queryParams.named()).entrySet()) {
        options.addQueryParameter(entry.getKey(), entry.getValue().getType());
      }
    } else if (queryParams.getKind() == Kind.POSITIONAL) {
      options.setParameterMode(ParameterMode.PARAMETER_POSITIONAL);
      for (Value param : (List<Value>) queryParams.positional()) {
        options.addPositionalQueryParameter(param.getType());
      }
    }

    return options;
  }
}
