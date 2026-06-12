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
package org.apache.beam.sdk.extensions.sql.impl;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters.Kind;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.udf.BeamBuiltinFunctionProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.calcite.v1_40_0.com.google.common.collect.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Contexts;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitDef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexDynamicParam;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlConformance;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Planner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Program;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelConversionException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.ValidationException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.BuiltInMethod;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core component to handle through a SQL statement, from explain execution plan, to generate a
 * Beam pipeline.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CalciteQueryPlanner implements QueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(CalciteQueryPlanner.class);

  private final Planner planner;
  private final JdbcConnection connection;
  private final FrameworkConfig config;

  /** Called by {@link BeamSqlEnv}.instantiatePlanner() reflectively. */
  public CalciteQueryPlanner(JdbcConnection connection, Collection<RuleSet> ruleSets) {
    this.connection = connection;
    this.config = defaultConfig(connection, ruleSets);
    this.planner = Frameworks.getPlanner(config);
  }

  /**
   * Returns a RelBuilder instance configured with the same Calcite components used by this
   * QueryPlanner.
   */
  @Override
  public RelBuilder getRelBuilder() {
    return RelBuilder.create(config);
  }

  @Override
  public SqlOperatorTable getOperatorTable() {
    return config.getOperatorTable();
  }

  public static final Factory FACTORY =
      new Factory() {
        @Override
        public QueryPlanner createPlanner(
            JdbcConnection jdbcConnection, Collection<RuleSet> ruleSets) {
          loadBuiltinFunctions(jdbcConnection);
          LOG.info("Factory creating planner with ruleSets: {}", ruleSets);
          return new CalciteQueryPlanner(jdbcConnection, ruleSets);
        }

        private void loadBuiltinFunctions(JdbcConnection jdbcConnection) {
          for (BeamBuiltinFunctionProvider provider :
              ServiceLoader.load(BeamBuiltinFunctionProvider.class)) {
            for (Map.Entry<String, List<Method>> entry : provider.getBuiltinMethods().entrySet()) {
              for (Method method : entry.getValue()) {
                jdbcConnection.getCurrentSchemaPlus().add(entry.getKey(), UdfImpl.create(method));
              }
            }
          }
        }
      };

  public FrameworkConfig defaultConfig(JdbcConnection connection, Collection<RuleSet> ruleSets) {
    final CalciteConnectionConfig config = connection.config();
    // Resolve the parser conformance. Calcite's Avatica JDBC connect path silently drops the
    // {@code conformance} connection property (it is not in the driver's registered property set),
    // so {@code config.conformance()} is always DEFAULT here even when callers set it via
    // {@code BeamSqlPipelineOptions.calciteConnectionProperties}. We therefore read that map
    // directly from the connection's pipeline options and let it override. This keeps the behavior
    // opt-in: with no {@code conformance} property the connection's own (DEFAULT) value is used, so
    // existing Beam SQL behavior is unchanged.
    final SqlConformance conformance = resolveConformance(connection, config);
    final SqlParser.ConfigBuilder parserConfig =
        SqlParser.configBuilder()
            .setQuotedCasing(config.quotedCasing())
            .setUnquotedCasing(config.unquotedCasing())
            .setQuoting(config.quoting())
            .setConformance(conformance)
            .setCaseSensitive(config.caseSensitive());
    final SqlParserImplFactory parserFactory =
        config.parserFactory(SqlParserImplFactory.class, null);
    if (parserFactory != null) {
      parserConfig.setParserFactory(parserFactory);
    }

    final SchemaPlus schema = connection.getRootSchema();
    final SchemaPlus defaultSchema = connection.getCurrentSchemaPlus();

    final ImmutableList<RelTraitDef> traitDefs = ImmutableList.of(ConventionTraitDef.INSTANCE);

    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(schema),
            ImmutableList.of(defaultSchema.getName()),
            connection.getTypeFactory(),
            connection.config());
    final SqlOperatorTable opTab0 =
        connection.config().fun(SqlOperatorTable.class, SqlStdOperatorTable.instance());

    // Revert the flag flip of CALCITE-3870 which led to missing rules
    SqlToRelConverter.Config sqlToRelConfig = SqlToRelConverter.config().withExpand(true);

    LOG.info("Creating config with rulesets: {}", ruleSets);
    return Frameworks.newConfigBuilder()
        .parserConfig(parserConfig.build())
        .defaultSchema(defaultSchema)
        .traitDefs(traitDefs)
        .context(Contexts.of(connection.config()))
        .ruleSets(ruleSets.toArray(new RuleSet[0]))
        .costFactory(BeamCostModel.FACTORY)
        .typeSystem(connection.getTypeFactory().getTypeSystem())
        .operatorTable(SqlOperatorTables.chain(opTab0, catalogReader))
        .sqlToRelConverterConfig(sqlToRelConfig)
        .build();
  }

  /**
   * Resolves the {@link SqlConformance} for the parser. Prefers an explicit {@code conformance}
   * entry in {@link BeamSqlPipelineOptions#getCalciteConnectionProperties()} (looked up
   * case-insensitively, value matched against {@link SqlConformanceEnum}); otherwise falls back to
   * the connection's own conformance. This is the bridge for {@code conformance=BABEL}, which the
   * Avatica JDBC connect path drops, enabling Spark-SQL spellings (e.g. the {@code !=} operator)
   * the default conformance rejects. Returns the connection default on any unrecognized value.
   */
  private static SqlConformance resolveConformance(
      JdbcConnection connection, CalciteConnectionConfig config) {
    PipelineOptions options = connection.getPipelineOptions();
    if (options == null) {
      return config.conformance();
    }
    BeamSqlPipelineOptions sqlOptions = options.as(BeamSqlPipelineOptions.class);
    Map<String, String> props = sqlOptions.getCalciteConnectionProperties();
    if (props == null) {
      return config.conformance();
    }
    String value = null;
    for (Map.Entry<String, String> e : props.entrySet()) {
      if ("conformance".equalsIgnoreCase(e.getKey())) {
        value = e.getValue();
        break;
      }
    }
    if (value == null) {
      return config.conformance();
    }
    try {
      return SqlConformanceEnum.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException ex) {
      LOG.warn("Unrecognized calcite conformance '{}', using {}", value, config.conformance());
      return config.conformance();
    }
  }

  /** Parse input SQL query, and return a {@link SqlNode} as grammar tree. */
  @Override
  public SqlNode parse(String sqlStatement) throws ParseException {
    SqlNode parsed;
    try {
      parsed = planner.parse(sqlStatement);
    } catch (SqlParseException e) {
      throw new ParseException(String.format("Unable to parse query %s", sqlStatement), e);
    } finally {
      planner.close();
    }
    return parsed;
  }

  /**
   * It parses and validate the input query, then convert into a {@link BeamRelNode} tree. Note that
   * query parameters are now supported for positional parameters.
   */
  @Override
  public BeamRelNode convertToBeamRel(String sqlStatement, QueryParameters queryParameters)
      throws ParseException, SqlConversionException {
    Preconditions.checkArgument(
        queryParameters.getKind() == Kind.NONE || queryParameters.getKind() == Kind.POSITIONAL,
        "Beam SQL Calcite dialect only supports positional query parameters or no parameters.");
    try {
      SqlNode parsed = planner.parse(sqlStatement);
      TableResolutionUtils.setupCustomTableResolution(connection, parsed);
      SqlNode validated = planner.validate(parsed);
      LOG.info("SQL:\n{}", validated);

      // root of original logical plan
      RelRoot root = planner.rel(validated);
      RelNode relNode = root.rel;
      if (queryParameters.getKind() == Kind.POSITIONAL) {
        relNode =
            bindParameters(
                relNode,
                new ParameterBinder(root.rel.getCluster().getRexBuilder(), queryParameters));
      }
      return convertToBeamRel(relNode, root.collation);
    } catch (RelConversionException | CannotPlanException e) {
      throw new SqlConversionException(
          String.format("Unable to convert query %s", sqlStatement), e);
    } catch (SqlParseException | ValidationException e) {
      throw new ParseException(String.format("Unable to parse query %s", sqlStatement), e);
    }
  }

  private static RelNode bindParameters(RelNode rel, RexShuttle binder) {
    RelNode newRel = rel.accept(binder);
    java.util.List<RelNode> newInputs = new java.util.ArrayList<>();
    for (RelNode input : newRel.getInputs()) {
      newInputs.add(bindParameters(input, binder));
    }
    return newRel.copy(newRel.getTraitSet(), newInputs);
  }

  @Override
  public RelNode parseToRel(String sqlStatement, QueryParameters queryParameters)
      throws ParseException, SqlConversionException {
    Preconditions.checkArgument(
        queryParameters.getKind() == Kind.NONE,
        "Beam SQL Calcite dialect does not yet support query parameters.");
    try {
      SqlNode parsed = planner.parse(sqlStatement);
      TableResolutionUtils.setupCustomTableResolution(connection, parsed);
      SqlNode validated = planner.validate(parsed);
      // root of original logical plan
      RelRoot root = planner.rel(validated);
      return root.rel;
    } catch (RelConversionException e) {
      throw new SqlConversionException(
          String.format("Unable to convert query %s", sqlStatement), e);
    } catch (SqlParseException | ValidationException e) {
      throw new ParseException(String.format("Unable to parse query %s", sqlStatement), e);
    }
  }

  @Override
  public BeamRelNode convertToBeamRel(RelNode relNode, QueryParameters queryParameters)
      throws SqlConversionException {
    if (queryParameters.getKind() == Kind.POSITIONAL) {
      relNode =
          bindParameters(
              relNode, new ParameterBinder(relNode.getCluster().getRexBuilder(), queryParameters));
    }
    return convertToBeamRel(relNode, (RelCollation) null);
  }

  private BeamRelNode convertToBeamRel(RelNode relNode, @Nullable RelCollation collation)
      throws SqlConversionException {
    BeamRelNode beamRelNode;
    try {
      LOG.info("SQLPlan>\n{}", BeamSqlRelUtils.explainLazily(relNode));
      RelTraitSet desiredTraits = relNode.getTraitSet().replace(BeamLogicalConvention.INSTANCE);
      if (collation != null) {
        desiredTraits = desiredTraits.replace(collation);
      }
      desiredTraits = desiredTraits.simplify();
      // beam physical plan
      relNode
          .getCluster()
          .setMetadataProvider(
              ChainedRelMetadataProvider.of(
                  ImmutableList.of(
                      NonCumulativeCostImpl.SOURCE,
                      RelMdNodeStats.SOURCE,
                      relNode.getCluster().getMetadataProvider())));

      relNode.getCluster().setMetadataQuerySupplier(BeamRelMetadataQuery::instance);
      RelMetadataQuery.THREAD_PROVIDERS.set(
          JaninoRelMetadataProvider.of(relNode.getCluster().getMetadataProvider()));
      relNode.getCluster().invalidateMetadataQuery();

      if (config.getPrograms().isEmpty()) {
        throw new SqlConversionException("No planning programs configured in FrameworkConfig.");
      }
      Program program = config.getPrograms().get(0);
      LOG.info("Desired traits: {}", desiredTraits);
      RelNode optimizedNode =
          program.run(
              relNode.getCluster().getPlanner(),
              relNode,
              desiredTraits,
              ImmutableList.of(),
              ImmutableList.of());
      LOG.info("BEAMPlan>\n{}", BeamSqlRelUtils.explainLazily(optimizedNode));

      if (!(optimizedNode instanceof BeamRelNode)) {
        throw new SqlConversionException(
            String.format(
                "The optimizer was unable to produce a Beam physical plan. "
                    + "Expected BeamRelNode, but got: %s",
                optimizedNode.getClass().getName()));
      }
      beamRelNode = (BeamRelNode) optimizedNode;
    } catch (CannotPlanException e) {
      throw new SqlConversionException(
          String.format("Unable to convert relNode to Beam: %s", relNode), e);
    } finally {
      planner.close();
    }
    return beamRelNode;
  }

  // It needs to be public so that the generated code in Calcite can access it.
  public static class NonCumulativeCostImpl
      implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.NON_CUMULATIVE_COST.method, new NonCumulativeCostImpl());

    @Override
    public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
      return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    @SuppressWarnings("UnusedDeclaration")
    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
      assert mq instanceof BeamRelMetadataQuery;
      BeamRelMetadataQuery bmq = (BeamRelMetadataQuery) mq;

      // This is called by a generated code in calcite MetadataQuery.
      // If the rel is Calcite rel or we are in JDBC path and cost factory is not set yet we should
      // use calcite cost estimation
      if (!(rel instanceof BeamRelNode)) {
        return rel.computeSelfCost(rel.getCluster().getPlanner(), bmq);
      }

      // Currently we do nothing in this case, however, we can plug our own cost estimation method
      // here and based on the design we also need to remove the cached values

      // We need to first remove the cached values.
      List<Table.Cell<RelNode, ?, Object>> costKeys =
          bmq.map.cellSet().stream()
              .filter(entry -> entry.getValue() instanceof BeamCostModel)
              .filter(entry -> ((BeamCostModel) entry.getValue()).isInfinite())
              .collect(Collectors.toList());

      costKeys.forEach(cell -> bmq.map.remove(cell.getRowKey(), cell.getColumnKey()));

      return ((BeamRelNode) rel).beamComputeSelfCost(rel.getCluster().getPlanner(), bmq);
    }
  }

  private static class ParameterBinder extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final List<?> positionalParams;

    ParameterBinder(RexBuilder rexBuilder, QueryParameters params) {
      this.rexBuilder = rexBuilder;
      this.positionalParams = params.getKind() == Kind.POSITIONAL ? params.positional() : null;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      if (positionalParams != null) {
        int index = dynamicParam.getIndex();
        if (index < 0 || index >= positionalParams.size()) {
          throw new IllegalArgumentException(
              "Index out of bounds for positional parameter: " + index);
        }
        Object val = positionalParams.get(index);
        return makeLiteral(cleanValue(val), dynamicParam.getType());
      }
      return super.visitDynamicParam(dynamicParam);
    }

    private RexNode makeLiteral(Object val, RelDataType type) {
      if (val == null) {
        return rexBuilder.makeNullLiteral(type);
      }
      return rexBuilder.makeLiteral(val, type, true);
    }

    @SuppressWarnings("JavaUtilDate") // explicit java.util.Date support
    private Object cleanValue(Object value) {
      if (value instanceof org.joda.time.ReadableInstant) {
        return ((org.joda.time.ReadableInstant) value).getMillis();
      }
      if (value instanceof java.time.LocalDate) {
        return (int) ((java.time.LocalDate) value).toEpochDay();
      }
      if (value instanceof java.time.LocalTime) {
        return (int) (((java.time.LocalTime) value).toNanoOfDay() / 1_000_000L);
      }
      if (value instanceof java.time.LocalDateTime) {
        return ((java.time.LocalDateTime) value).toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
      }
      if (value instanceof java.sql.Timestamp) {
        return ((java.sql.Timestamp) value).getTime();
      }
      if (value instanceof java.util.Date) {
        return ((java.util.Date) value).getTime();
      }
      return value;
    }
  }
}
