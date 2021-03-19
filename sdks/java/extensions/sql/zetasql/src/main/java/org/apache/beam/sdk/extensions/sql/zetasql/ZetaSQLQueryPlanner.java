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

import com.google.zetasql.LanguageOptions;
import com.google.zetasql.Value;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.NonCumulativeCostImpl;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUncollectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnnestRule;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ZetaSqlScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.zetasql.unnest.BeamZetaSqlUncollectRule;
import org.apache.beam.sdk.extensions.sql.zetasql.unnest.BeamZetaSqlUnnestRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ZetaSQLQueryPlanner. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ZetaSQLQueryPlanner implements QueryPlanner {
  // TODO(BEAM-11747) Re-enable BeamJavaUdfCalcRule by default when it is safe to do so.
  public static final Collection<RelOptRule> DEFAULT_CALC =
      ImmutableList.<RelOptRule>builder()
          .add(BeamZetaSqlCalcRule.INSTANCE, BeamZetaSqlCalcMergeRule.INSTANCE)
          .build();

  private static final Logger LOG = LoggerFactory.getLogger(ZetaSQLQueryPlanner.class);

  private final ZetaSQLPlannerImpl plannerImpl;

  public ZetaSQLQueryPlanner(FrameworkConfig config) {
    plannerImpl = new ZetaSQLPlannerImpl(config);
  }

  /**
   * Called by {@link org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv}.instantiatePlanner()
   * reflectively.
   */
  public ZetaSQLQueryPlanner(JdbcConnection jdbcConnection, Collection<RuleSet> ruleSets) {
    plannerImpl =
        new ZetaSQLPlannerImpl(
            defaultConfig(jdbcConnection, modifyRuleSetsForZetaSql(ruleSets, DEFAULT_CALC)));
    setDefaultTimezone(
        jdbcConnection
            .getPipelineOptions()
            .as(BeamSqlPipelineOptions.class)
            .getZetaSqlDefaultTimezone());
  }

  public static final Factory FACTORY =
      new Factory() {
        @Override
        public QueryPlanner createPlanner(
            JdbcConnection jdbcConnection, Collection<RuleSet> ruleSets) {
          return new ZetaSQLQueryPlanner(jdbcConnection, ruleSets);
        }
      };

  public static Collection<RuleSet> getZetaSqlRuleSets() {
    return modifyRuleSetsForZetaSql(BeamRuleSets.getRuleSets(), DEFAULT_CALC);
  }

  public static Collection<RuleSet> getZetaSqlRuleSets(Collection<RelOptRule> calc) {
    return modifyRuleSetsForZetaSql(BeamRuleSets.getRuleSets(), calc);
  }

  /**
   * Returns true if all the following are true: All RexCalls can be implemented by codegen, All
   * RexCalls only contain ZetaSQL user-defined Java functions, All RexLiterals pass ZetaSQL
   * compliance tests, All RexInputRefs pass ZetaSQL compliance tests, No other RexNode types
   * Otherwise returns false. ZetaSQL user-defined Java functions are in the category whose function
   * group is equal to {@code SqlAnalyzer.USER_DEFINED_JAVA_SCALAR_FUNCTIONS}
   */
  static boolean hasOnlyJavaUdfInProjects(RelOptRuleCall x) {
    List<RelNode> resList = x.getRelList();
    for (RelNode relNode : resList) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            final SqlOperator operator = call.getOperator();

            CallImplementor implementor = RexImpTable.INSTANCE.get(operator);
            if (implementor == null) {
              // Reject methods with no implementation
              return false;
            }

            if (operator instanceof SqlUserDefinedFunction) {
              SqlUserDefinedFunction udf = (SqlUserDefinedFunction) call.op;
              if (udf.function instanceof ZetaSqlScalarFunctionImpl) {
                ZetaSqlScalarFunctionImpl scalarFunction = (ZetaSqlScalarFunctionImpl) udf.function;
                if (!scalarFunction.functionGroup.equals(
                    BeamZetaSqlCatalog.USER_DEFINED_JAVA_SCALAR_FUNCTIONS)) {
                  // Reject ZetaSQL Builtin Scalar Functions
                  return false;
                }
              } else {
                // Reject other UDFs
                return false;
              }
            } else {
              // Reject Calcite implementations
              return false;
            }
          } else if (rexNode instanceof RexLiteral) {
            SqlTypeName typeName = ((RexLiteral) rexNode).getTypeName();
            switch (typeName) {
              case NULL:
              case BOOLEAN:
              case CHAR:
              case BINARY:
              case DECIMAL:
                break;
              default:
                // Reject unsupported literals
                return false;
            }
          } else if (rexNode instanceof RexInputRef) {
            SqlTypeName typeName = ((RexInputRef) rexNode).getType().getSqlTypeName();
            switch (typeName) {
              case BIGINT:
              case DOUBLE:
              case BOOLEAN:
              case VARCHAR:
              case VARBINARY:
              case DECIMAL:
                break;
              default:
                // Reject unsupported input ref
                return false;
            }
          } else {
            // Reject everything else
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Returns false if the argument contains any user-defined Java functions, otherwise returns true.
   */
  static boolean hasNoJavaUdfInProjects(RelOptRuleCall x) {
    List<RelNode> resList = x.getRelList();
    for (RelNode relNode : resList) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperator() instanceof SqlUserDefinedFunction) {
              SqlUserDefinedFunction udf = (SqlUserDefinedFunction) call.op;
              if (udf.function instanceof ZetaSqlScalarFunctionImpl) {
                ZetaSqlScalarFunctionImpl scalarFunction = (ZetaSqlScalarFunctionImpl) udf.function;
                if (scalarFunction.functionGroup.equals(
                    BeamZetaSqlCatalog.USER_DEFINED_JAVA_SCALAR_FUNCTIONS)) {
                  return false;
                }
              }
            }
          }
        }
      }
    }
    return true;
  }

  private static Collection<RuleSet> modifyRuleSetsForZetaSql(
      Collection<RuleSet> ruleSets, Collection<RelOptRule> calc) {
    ImmutableList.Builder<RuleSet> ret = ImmutableList.builder();
    for (RuleSet ruleSet : ruleSets) {
      ImmutableList.Builder<RelOptRule> bd = ImmutableList.builder();
      for (RelOptRule rule : ruleSet) {
        // TODO[BEAM-9075]: Fix join re-ordering for ZetaSQL planner. Currently join re-ordering
        //  requires the JoinCommuteRule, which doesn't work without struct flattening.
        if (rule instanceof JoinCommuteRule) {
          continue;
        } else if (rule instanceof FilterCalcMergeRule || rule instanceof ProjectCalcMergeRule) {
          // In order to support Java UDF, we need both BeamZetaSqlCalcRel and BeamCalcRel. It is
          // because BeamZetaSqlCalcRel can execute ZetaSQL built-in functions while BeamCalcRel
          // can execute UDFs. So during planning, we expect both Filter and Project are converted
          // to Calc nodes before merging with other Project/Filter/Calc nodes. Thus we should not
          // add FilterCalcMergeRule and ProjectCalcMergeRule. CalcMergeRule will achieve equivalent
          // planning result eventually.
          continue;
        } else if (rule instanceof BeamCalcRule) {
          bd.addAll(calc);
        } else if (rule instanceof BeamUnnestRule) {
          bd.add(BeamZetaSqlUnnestRule.INSTANCE);
        } else if (rule instanceof BeamUncollectRule) {
          bd.add(BeamZetaSqlUncollectRule.INSTANCE);
        } else {
          bd.add(rule);
        }
      }
      ret.add(RuleSets.ofList(bd.build()));
    }
    return ret.build();
  }

  public String getDefaultTimezone() {
    return plannerImpl.getDefaultTimezone();
  }

  public void setDefaultTimezone(String timezone) {
    plannerImpl.setDefaultTimezone(timezone);
  }

  public static LanguageOptions getLanguageOptions() {
    return ZetaSQLPlannerImpl.getLanguageOptions();
  }

  public BeamRelNode convertToBeamRel(String sqlStatement) {
    return convertToBeamRel(sqlStatement, QueryParameters.ofNone());
  }

  public BeamRelNode convertToBeamRel(String sqlStatement, Map<String, Value> queryParams)
      throws ParseException, SqlConversionException {
    return convertToBeamRel(sqlStatement, QueryParameters.ofNamed(queryParams));
  }

  public BeamRelNode convertToBeamRel(String sqlStatement, List<Value> queryParams)
      throws ParseException, SqlConversionException {
    return convertToBeamRel(sqlStatement, QueryParameters.ofPositional(queryParams));
  }

  @Override
  public BeamRelNode convertToBeamRel(String sqlStatement, QueryParameters queryParameters)
      throws ParseException, SqlConversionException {
    return convertToBeamRelInternal(sqlStatement, queryParameters);
  }

  @Override
  public SqlNode parse(String sqlStatement) throws ParseException {
    throw new UnsupportedOperationException(
        String.format(
            "%s.parse(String) is not implemented and should need be called",
            this.getClass().getCanonicalName()));
  }

  private BeamRelNode convertToBeamRelInternal(String sql, QueryParameters queryParams) {
    RelRoot root = plannerImpl.rel(sql, queryParams);
    RelTraitSet desiredTraits =
        root.rel
            .getTraitSet()
            .replace(BeamLogicalConvention.INSTANCE)
            .replace(root.collation)
            .simplify();
    // beam physical plan
    root.rel
        .getCluster()
        .setMetadataProvider(
            ChainedRelMetadataProvider.of(
                org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList.of(
                    NonCumulativeCostImpl.SOURCE,
                    RelMdNodeStats.SOURCE,
                    root.rel.getCluster().getMetadataProvider())));
    RelMetadataQuery.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(root.rel.getCluster().getMetadataProvider()));
    root.rel.getCluster().invalidateMetadataQuery();
    BeamRelNode beamRelNode = (BeamRelNode) plannerImpl.transform(0, desiredTraits, root.rel);
    LOG.info("BEAMPlan>\n" + RelOptUtil.toString(beamRelNode));
    return beamRelNode;
  }

  private static FrameworkConfig defaultConfig(
      JdbcConnection connection, Collection<RuleSet> ruleSets) {
    final CalciteConnectionConfig config = connection.config();
    final SqlParser.ConfigBuilder parserConfig =
        SqlParser.configBuilder()
            .setQuotedCasing(config.quotedCasing())
            .setUnquotedCasing(config.unquotedCasing())
            .setQuoting(config.quoting())
            .setConformance(config.conformance())
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

    return Frameworks.newConfigBuilder()
        .parserConfig(parserConfig.build())
        .defaultSchema(defaultSchema)
        .traitDefs(traitDefs)
        .ruleSets(ruleSets.toArray(new RuleSet[0]))
        .costFactory(BeamCostModel.FACTORY)
        .typeSystem(connection.getTypeFactory().getTypeSystem())
        .operatorTable(ChainedSqlOperatorTable.of(opTab0, catalogReader))
        .build();
  }
}
