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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Function;
import com.google.zetasql.FunctionArgumentType;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.TVFRelation;
import com.google.zetasql.TableValuedFunction;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Mode;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLOptions.ErrorMessageMode;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLOptions.ParameterMode;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCreateTableFunctionStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters.Kind;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.utils.TVFStreamingUtils;
import org.apache.beam.sdk.extensions.sql.zetasql.TableResolution.SimpleTableWithPath;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;

/** Adapter for {@link Analyzer} to simplify the API for parsing the query and resolving the AST. */
public class SqlAnalyzer {
  public static final String PRE_DEFINED_WINDOW_FUNCTIONS = "pre_defined_window_functions";
  public static final String USER_DEFINED_FUNCTIONS = "user_defined_functions";

  private static final ImmutableSet<ResolvedNodeKind> SUPPORTED_STATEMENT_KINDS =
      ImmutableSet.of(
          RESOLVED_QUERY_STMT, RESOLVED_CREATE_FUNCTION_STMT, RESOLVED_CREATE_TABLE_FUNCTION_STMT);

  private static final ImmutableList<String> FUNCTION_LIST =
      ImmutableList.of(
          // TODO: support optional function argument (for window_offset).
          "CREATE FUNCTION TUMBLE(ts TIMESTAMP, window_size STRING) AS (1);",
          "CREATE FUNCTION TUMBLE_START(window_size STRING) RETURNS TIMESTAMP AS (null);",
          "CREATE FUNCTION TUMBLE_END(window_size STRING) RETURNS TIMESTAMP AS (null);",
          "CREATE FUNCTION HOP(ts TIMESTAMP, emit_frequency STRING, window_size STRING) AS (1);",
          "CREATE FUNCTION HOP_START(emit_frequency STRING, window_size STRING) "
              + "RETURNS TIMESTAMP AS (null);",
          "CREATE FUNCTION HOP_END(emit_frequency STRING, window_size STRING) "
              + "RETURNS TIMESTAMP AS (null);",
          "CREATE FUNCTION SESSION(ts TIMESTAMP, session_gap STRING) AS (1);",
          "CREATE FUNCTION SESSION_START(session_gap STRING) RETURNS TIMESTAMP AS (null);",
          "CREATE FUNCTION SESSION_END(session_gap STRING) RETURNS TIMESTAMP AS (null);");

  private final QueryTrait queryTrait;
  private final SchemaPlus topLevelSchema;
  private final JavaTypeFactory typeFactory;

  SqlAnalyzer(QueryTrait queryTrait, SchemaPlus topLevelSchema, JavaTypeFactory typeFactory) {
    this.queryTrait = queryTrait;
    this.topLevelSchema = topLevelSchema;
    this.typeFactory = typeFactory;
  }

  static boolean isEndOfInput(ParseResumeLocation parseResumeLocation) {
    return parseResumeLocation.getBytePosition()
        >= parseResumeLocation.getInput().getBytes(UTF_8).length;
  }

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
   * Accepts the ParseResumeLocation for the current position in the SQL string. Advances the
   * ParseResumeLocation to the start of the next statement. Adds user-defined functions to the
   * catalog for use in following statements. Returns the resolved AST.
   */
  ResolvedStatement analyzeNextStatement(
      ParseResumeLocation parseResumeLocation, AnalyzerOptions options, SimpleCatalog catalog) {
    ResolvedStatement resolvedStatement =
        Analyzer.analyzeNextStatement(parseResumeLocation, options, catalog);
    if (resolvedStatement.nodeKind() == RESOLVED_CREATE_FUNCTION_STMT) {
      ResolvedCreateFunctionStmt createFunctionStmt =
          (ResolvedCreateFunctionStmt) resolvedStatement;
      Function userFunction =
          new Function(
              createFunctionStmt.getNamePath(),
              USER_DEFINED_FUNCTIONS,
              // TODO(BEAM-9954) handle aggregate functions
              // TODO(BEAM-9969) handle table functions
              Mode.SCALAR,
              ImmutableList.of(createFunctionStmt.getSignature()));
      try {
        catalog.addFunction(userFunction);
      } catch (IllegalArgumentException e) {
        throw new ParseException(
            String.format(
                "Failed to define function %s", String.join(".", createFunctionStmt.getNamePath())),
            e);
      }
    } else if (resolvedStatement.nodeKind() == RESOLVED_CREATE_TABLE_FUNCTION_STMT) {
      ResolvedCreateTableFunctionStmt createTableFunctionStmt =
          (ResolvedCreateTableFunctionStmt) resolvedStatement;
      catalog.addTableValuedFunction(
          new TableValuedFunction.FixedOutputSchemaTVF(
              createTableFunctionStmt.getNamePath(),
              createTableFunctionStmt.getSignature(),
              TVFRelation.createColumnBased(
                  createTableFunctionStmt.getQuery().getColumnList().stream()
                      .map(c -> TVFRelation.Column.create(c.getName(), c.getType()))
                      .collect(Collectors.toList()))));
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
                    LanguageFeature.FEATURE_NUMERIC_TYPE,
                    LanguageFeature.FEATURE_DISALLOW_GROUP_BY_FLOAT,
                    LanguageFeature.FEATURE_V_1_2_CIVIL_TIME,
                    LanguageFeature.FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE,
                    LanguageFeature.FEATURE_TABLE_VALUED_FUNCTIONS,
                    LanguageFeature.FEATURE_CREATE_TABLE_FUNCTION,
                    LanguageFeature.FEATURE_TEMPLATE_FUNCTIONS)));
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

  /**
   * Creates a SimpleCatalog which represents the top-level schema, populates it with tables,
   * built-in functions.
   */
  SimpleCatalog createPopulatedCatalog(
      String catalogName, AnalyzerOptions options, List<List<String>> tables) {

    SimpleCatalog catalog = new SimpleCatalog(catalogName);
    addBuiltinFunctionsToCatalog(catalog, options);

    tables.forEach(table -> addTableToLeafCatalog(queryTrait, catalog, table));

    return catalog;
  }

  //TODO: Fix Later
  @SuppressWarnings("nullness")
  private void addBuiltinFunctionsToCatalog(SimpleCatalog catalog, AnalyzerOptions options) {
    // Enable ZetaSQL builtin functions.
    ZetaSQLBuiltinFunctionOptions zetasqlBuiltinFunctionOptions =
        new ZetaSQLBuiltinFunctionOptions(options.getLanguageOptions());

    SupportedZetaSqlBuiltinFunctions.ALLOWLIST.forEach(
        zetasqlBuiltinFunctionOptions::includeFunctionSignatureId);

    catalog.addZetaSQLFunctions(zetasqlBuiltinFunctionOptions);

    FUNCTION_LIST.stream()
        .map(func -> (ResolvedCreateFunctionStmt) Analyzer.analyzeStatement(func, options, catalog))
        .map(
            resolvedFunc ->
                new Function(
                    String.join(".", resolvedFunc.getNamePath()),
                    PRE_DEFINED_WINDOW_FUNCTIONS,
                    Mode.SCALAR,
                    ImmutableList.of(resolvedFunc.getSignature())))
        .forEach(catalog::addFunction);

    FunctionArgumentType retType =
        new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION);

    FunctionArgumentType inputTableType =
        new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION);

    FunctionArgumentType descriptorType =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_DESCRIPTOR,
            FunctionArgumentType.FunctionArgumentTypeOptions.builder()
                .setDescriptorResolutionTableOffset(0)
                .build(),
            1);

    FunctionArgumentType stringType =
        new FunctionArgumentType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING));

    // TUMBLE
    catalog.addTableValuedFunction(
        new TableValuedFunction.ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
            ImmutableList.of(TVFStreamingUtils.FIXED_WINDOW_TVF),
            new FunctionSignature(
                retType, ImmutableList.of(inputTableType, descriptorType, stringType), -1),
            ImmutableList.of(
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_START,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_END,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP))),
            null,
            null));

    // HOP
    catalog.addTableValuedFunction(
        new TableValuedFunction.ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
            ImmutableList.of(TVFStreamingUtils.SLIDING_WINDOW_TVF),
            new FunctionSignature(
                retType,
                ImmutableList.of(inputTableType, descriptorType, stringType, stringType),
                -1),
            ImmutableList.of(
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_START,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_END,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP))),
            null,
            null));

    // SESSION
    catalog.addTableValuedFunction(
        new TableValuedFunction.ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
            ImmutableList.of(TVFStreamingUtils.SESSION_WINDOW_TVF),
            new FunctionSignature(
                retType,
                ImmutableList.of(inputTableType, descriptorType, descriptorType, stringType),
                -1),
            ImmutableList.of(
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_START,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                TVFRelation.Column.create(
                    TVFStreamingUtils.WINDOW_END,
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP))),
            null,
            null));
  }

  /**
   * Assume last element in tablePath is a table name, and everything before is catalogs. So the
   * logic is to create nested catalogs until the last level, then add a table at the last level.
   *
   * <p>Table schema is extracted from Calcite schema based on the table name resultion strategy,
   * e.g. either by drilling down the schema.getSubschema() path or joining the table name with dots
   * to construct a single compound identifier (e.g. Data Catalog use case).
   */
  private void addTableToLeafCatalog(
      QueryTrait trait, SimpleCatalog topLevelCatalog, List<String> tablePath) {

    SimpleCatalog leafCatalog = createNestedCatalogs(topLevelCatalog, tablePath);

    org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.Table calciteTable =
        TableResolution.resolveCalciteTable(topLevelSchema, tablePath);

    if (calciteTable == null) {
      throw new SqlConversionException(
          "Wasn't able to resolve the path "
              + tablePath
              + " in schema: "
              + topLevelSchema.getName());
    }

    RelDataType rowType = calciteTable.getRowType(typeFactory);

    SimpleTableWithPath tableWithPath = SimpleTableWithPath.of(tablePath);
    trait.addResolvedTable(tableWithPath);

    addFieldsToTable(tableWithPath, rowType);
    leafCatalog.addSimpleTable(tableWithPath.getTable());
  }

  private void addFieldsToTable(SimpleTableWithPath tableWithPath, RelDataType rowType) {
    for (RelDataTypeField field : rowType.getFieldList()) {
      tableWithPath
          .getTable()
          .addSimpleColumn(
              field.getName(), ZetaSqlCalciteTranslationUtils.toZetaSqlType(field.getType()));
    }
  }

  /** For table path like a.b.c we assume c is the table and a.b are the nested catalogs/schemas. */
  private SimpleCatalog createNestedCatalogs(SimpleCatalog catalog, List<String> tablePath) {
    SimpleCatalog currentCatalog = catalog;
    for (int i = 0; i < tablePath.size() - 1; i++) {
      String nextCatalogName = tablePath.get(i);

      Optional<SimpleCatalog> existing = tryGetExisting(currentCatalog, nextCatalogName);

      currentCatalog =
          existing.isPresent() ? existing.get() : addNewCatalog(currentCatalog, nextCatalogName);
    }
    return currentCatalog;
  }

  private Optional<SimpleCatalog> tryGetExisting(
      SimpleCatalog currentCatalog, String nextCatalogName) {
    return currentCatalog.getCatalogList().stream()
        .filter(c -> nextCatalogName.equals(c.getFullName()))
        .findFirst();
  }

  private SimpleCatalog addNewCatalog(SimpleCatalog currentCatalog, String nextCatalogName) {
    SimpleCatalog nextCatalog = new SimpleCatalog(nextCatalogName);
    currentCatalog.addSimpleCatalog(nextCatalog);
    return nextCatalog;
  }
}
