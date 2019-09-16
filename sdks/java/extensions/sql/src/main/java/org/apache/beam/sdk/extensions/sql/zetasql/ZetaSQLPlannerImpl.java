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

import com.google.common.collect.ImmutableList;
import com.google.zetasql.Value;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import java.io.Reader;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ConversionContext;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ExpressionConverter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.QueryStatementConverter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

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
    return null;
  }

  @Override
  public SqlNode parse(Reader reader) throws SqlParseException {
    return null;
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) throws ValidationException {
    return null;
  }

  @Override
  public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
    throw new RuntimeException("validateAndGetType(SqlNode) is not implemented.");
  }

  @Override
  public RelRoot rel(SqlNode sqlNode) throws RelConversionException {
    return null;
  }

  public RelRoot rel(String sql, Map<String, Value> params) {
    this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    this.expressionConverter = new ExpressionConverter(cluster, params);

    QueryTrait trait = new QueryTrait();

    ResolvedStatement statement =
        SqlAnalyzer.withQueryParams(params)
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
    throw new RuntimeException("convert(SqlNode) is not implemented.");
  }

  @Override
  public RelDataTypeFactory getTypeFactory() {
    throw new RuntimeException("getTypeFactory() is not implemented.");
  }

  @Override
  public RelNode transform(int i, RelTraitSet relTraitSet, RelNode relNode)
      throws RelConversionException {
    relNode
        .getCluster()
        .setMetadataProvider(
            new CachingRelMetadataProvider(
                relNode.getCluster().getMetadataProvider(), relNode.getCluster().getPlanner()));
    Program program = programs.get(i);
    return program.run(planner, relNode, relTraitSet, ImmutableList.of(), ImmutableList.of());
  }

  @Override
  public void reset() {
    throw new RuntimeException("reset() is not implemented.");
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public RelTraitSet getEmptyTraitSet() {
    throw new RuntimeException("getEmptyTraitSet() is not implemented.");
  }
}
