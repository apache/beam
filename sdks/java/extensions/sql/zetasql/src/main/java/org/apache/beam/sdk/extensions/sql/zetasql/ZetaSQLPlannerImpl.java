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

import com.google.zetasql.Analyzer;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.io.Reader;
import java.util.List;
import java.util.logging.Logger;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ConversionContext;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ExpressionConverter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.QueryStatementConverter;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexExecutor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Planner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Program;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelConversionException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.ValidationException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Util;

/** ZetaSQLPlannerImpl. */
public class ZetaSQLPlannerImpl implements Planner {
  private static final Logger logger = Logger.getLogger(ZetaSQLPlannerImpl.class.getName());

  private final SchemaPlus defaultSchemaPlus;

  // variables that are used in Calcite's planner.
  private final FrameworkConfig config;
  private RelOptPlanner planner;
  private JavaTypeFactory typeFactory;
  private final RexExecutor executor;
  private RelOptCluster cluster;
  private final ImmutableList<Program> programs;
  private ExpressionConverter expressionConverter;

  private static final long ONE_SECOND_IN_MILLIS = 1000L;
  private static final long ONE_MINUTE_IN_MILLIS = 60L * ONE_SECOND_IN_MILLIS;
  private static final long ONE_HOUR_IN_MILLIS = 60L * ONE_MINUTE_IN_MILLIS;
  private static final long ONE_DAY_IN_MILLIS = 24L * ONE_HOUR_IN_MILLIS;

  @SuppressWarnings("unused")
  private static final long ONE_MONTH_IN_MILLIS = 30L * ONE_DAY_IN_MILLIS;

  @SuppressWarnings("unused")
  private static final long ONE_YEAR_IN_MILLIS = 365L * ONE_DAY_IN_MILLIS;

  public ZetaSQLPlannerImpl(FrameworkConfig config) {
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

  @Override
  public SqlNode parse(String s) throws SqlParseException {
    throw new UnsupportedOperationException(
        String.format("%s.parse(String) is not implemented", this.getClass().getCanonicalName()));
  }

  @Override
  public SqlNode parse(Reader reader) throws SqlParseException {
    throw new UnsupportedOperationException(
        String.format("%s.parse(Reader) is not implemented", this.getClass().getCanonicalName()));
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) throws ValidationException {
    throw new UnsupportedOperationException(
        String.format(
            "%s.validate(SqlNode) is not implemented", this.getClass().getCanonicalName()));
  }

  @Override
  public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
    throw new UnsupportedOperationException(
        String.format(
            "%s.validateAndGetType(SqlNode) is not implemented",
            this.getClass().getCanonicalName()));
  }

  @Override
  public RelRoot rel(SqlNode sqlNode) throws RelConversionException {
    throw new UnsupportedOperationException(
        String.format("%s.rel(SqlNode) is not implemented", this.getClass().getCanonicalName()));
  }

  public RelRoot rel(String sql, QueryParameters params) {
    this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    this.expressionConverter = new ExpressionConverter(cluster, params);

    QueryTrait trait = new QueryTrait();

    // Set up table providers that need to be pre-registered
    // TODO(https://issues.apache.org/jira/browse/BEAM-8817): share this logic between dialects
    List<List<String>> tables = Analyzer.extractTableNamesFromStatement(sql);
    TableResolution.registerTables(this.defaultSchemaPlus, tables);

    ResolvedStatement statement =
        SqlAnalyzer.getBuilder()
            .withQueryParams(params)
            .withQueryTrait(trait)
            .withCalciteContext(config.getContext())
            .withTopLevelSchema(defaultSchemaPlus)
            .withTypeFactory((JavaTypeFactory) cluster.getTypeFactory())
            .analyze(sql);

    if (!(statement instanceof ResolvedQueryStmt)) {
      throw new UnsupportedOperationException(
          "Unsupported query statement type: " + sql.getClass().getSimpleName());
    }

    ConversionContext context = ConversionContext.of(config, expressionConverter, cluster, trait);

    RelNode convertedNode =
        QueryStatementConverter.convertRootQuery(context, (ResolvedQueryStmt) statement);
    return RelRoot.of(convertedNode, SqlKind.ALL);
  }

  @Override
  public RelNode convert(SqlNode sqlNode) {
    throw new UnsupportedOperationException(
        String.format("%s.convert(SqlNode) is not implemented.", getClass().getCanonicalName()));
  }

  @Override
  public RelDataTypeFactory getTypeFactory() {
    throw new UnsupportedOperationException(
        String.format("%s.getTypeFactor() is not implemented.", getClass().getCanonicalName()));
  }

  @Override
  public RelNode transform(int i, RelTraitSet relTraitSet, RelNode relNode)
      throws RelConversionException {
    Program program = programs.get(i);
    return program.run(planner, relNode, relTraitSet, ImmutableList.of(), ImmutableList.of());
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException(
        String.format("%s.reset() is not implemented", this.getClass().getCanonicalName()));
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public RelTraitSet getEmptyTraitSet() {
    throw new UnsupportedOperationException(
        String.format(
            "%s.getEmptyTraitSet() is not implemented", this.getClass().getCanonicalName()));
  }

  public static LanguageOptions getLanguageOptions() {
    return SqlAnalyzer.initAnalyzerOptions().getLanguageOptions();
  }
}
