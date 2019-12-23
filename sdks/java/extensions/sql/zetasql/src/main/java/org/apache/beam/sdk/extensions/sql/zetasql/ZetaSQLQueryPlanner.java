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

import com.google.zetasql.Value;
import java.util.Collections;
import java.util.Map;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelConversionException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** ZetaSQLQueryPlanner. */
public class ZetaSQLQueryPlanner implements QueryPlanner {
  private final ZetaSQLPlannerImpl plannerImpl;

  public ZetaSQLQueryPlanner(FrameworkConfig config) {
    plannerImpl = new ZetaSQLPlannerImpl(config);
  }

  public ZetaSQLQueryPlanner(JdbcConnection jdbcConnection, RuleSet[] ruleSets) {
    // TODO[BEAM-8630]: uncomment the next lines once we have fully migrated to BeamZetaSqlCalcRel
    // plannerImpl =
    //     new ZetaSQLPlannerImpl(defaultConfig(jdbcConnection, replaceBeamCalcRule(ruleSets)));
    plannerImpl = new ZetaSQLPlannerImpl(defaultConfig(jdbcConnection, ruleSets));
  }

  public static RuleSet[] getZetaSqlRuleSets() {
    // TODO[BEAM-8630]: uncomment the next line once we have fully migrated to BeamZetaSqlCalcRel
    // return replaceBeamCalcRule(BeamRuleSets.getRuleSets());
    return BeamRuleSets.getRuleSets();
  }

  private static RuleSet[] replaceBeamCalcRule(RuleSet[] ruleSets) {
    RuleSet[] ret = new RuleSet[ruleSets.length];
    for (int i = 0; i < ruleSets.length; i++) {
      ImmutableList.Builder<RelOptRule> bd = ImmutableList.builder();
      for (RelOptRule rule : ruleSets[i]) {
        if (rule instanceof BeamCalcRule) {
          bd.add(BeamZetaSqlCalcRule.INSTANCE);
        } else {
          bd.add(rule);
        }
      }
      ret[i] = RuleSets.ofList(bd.build());
    }
    return ret;
  }

  @Override
  public BeamRelNode convertToBeamRel(String sqlStatement)
      throws ParseException, SqlConversionException {
    try {
      return parseQuery(sqlStatement);
    } catch (RelConversionException e) {
      throw new SqlConversionException(e.getCause());
    }
  }

  @Override
  public SqlNode parse(String sqlStatement) throws ParseException {
    throw new UnsupportedOperationException(
        String.format(
            "%s.parse(String) is not implemented and should need be called",
            this.getClass().getCanonicalName()));
  }

  public BeamRelNode convertToBeamRel(String sqlStatement, Map<String, Value> queryParams)
      throws ParseException, SqlConversionException {
    try {
      return parseQuery(sqlStatement, queryParams);
    } catch (RelConversionException e) {
      throw new SqlConversionException(e.getCause());
    }
  }

  public BeamRelNode parseQuery(String sql) throws RelConversionException {
    return parseQuery(sql, Collections.emptyMap());
  }

  public BeamRelNode parseQuery(String sql, Map<String, Value> queryParams)
      throws RelConversionException {
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
    return beamRelNode;
  }

  private FrameworkConfig defaultConfig(JdbcConnection connection, RuleSet[] ruleSets) {
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
        .ruleSets(ruleSets)
        .costFactory(BeamCostModel.FACTORY)
        .typeSystem(connection.getTypeFactory().getTypeSystem())
        .operatorTable(ChainedSqlOperatorTable.of(opTab0, catalogReader))
        .build();
  }
}
