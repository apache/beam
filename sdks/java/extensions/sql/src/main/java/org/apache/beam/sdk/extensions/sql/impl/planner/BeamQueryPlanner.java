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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core component to handle through a SQL statement, from explain execution plan, to generate a
 * Beam pipeline.
 */
public class BeamQueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(BeamQueryPlanner.class);

  private final FrameworkConfig config;

  public BeamQueryPlanner(CalciteConnection connection) {
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
    final SchemaPlus defaultSchema = JdbcDriver.getDefaultSchema(connection);

    final ImmutableList<RelTraitDef> traitDefs =
        ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);

    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(schema),
            ImmutableList.of(defaultSchema.getName()),
            connection.getTypeFactory(),
            connection.config());
    final SqlOperatorTable opTab0 =
        connection.config().fun(SqlOperatorTable.class, SqlStdOperatorTable.instance());

    this.config =
        Frameworks.newConfigBuilder()
            .parserConfig(parserConfig.build())
            .defaultSchema(defaultSchema)
            .traitDefs(traitDefs)
            .context(Contexts.of(connection.config()))
            .ruleSets(BeamRuleSets.getRuleSets())
            .costFactory(null)
            .typeSystem(connection.getTypeFactory().getTypeSystem())
            .operatorTable(ChainedSqlOperatorTable.of(opTab0, catalogReader))
            .build();
  }

  /** Parse input SQL query, and return a {@link SqlNode} as grammar tree. */
  public SqlNode parse(String sqlStatement) throws SqlParseException {
    Planner planner = getPlanner();
    SqlNode parsed;
    try {
      parsed = planner.parse(sqlStatement);
    } finally {
      planner.close();
    }
    return parsed;
  }

  /**
   * {@code compileBeamPipeline} translate a SQL statement to executed as Beam data flow, which is
   * linked with the given {@code pipeline}. The final output stream is returned as {@code
   * PCollection} so more operations can be applied.
   */
  public PCollection<Row> compileBeamPipeline(String sqlStatement, Pipeline basePipeline)
      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode relNode = convertToBeamRel(sqlStatement);

    // the input PCollectionTuple is empty, and be rebuilt in BeamIOSourceRel.
    return PCollectionTuple.empty(basePipeline).apply(relNode.toPTransform());
  }

  /** It parses and validate the input query, then convert into a {@link BeamRelNode} tree. */
  public BeamRelNode convertToBeamRel(String sqlStatement)
      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode beamRelNode;
    Planner planner = getPlanner();
    try {
      SqlNode parsed = planner.parse(sqlStatement);
      SqlNode validated = planner.validate(parsed);
      LOG.info("SQL:\n" + validated);

      RelRoot root = planner.rel(validated);
      LOG.info("SQLPlan>\n" + RelOptUtil.toString(root.rel));

      RelTraitSet desiredTraits =
          root.rel
              .getTraitSet()
              .replace(BeamLogicalConvention.INSTANCE)
              .replace(root.collation)
              .simplify();
      beamRelNode = (BeamRelNode) planner.transform(0, desiredTraits, root.rel);
      LOG.info("BeamSQL>\n" + RelOptUtil.toString(beamRelNode));
    } finally {
      planner.close();
    }
    return beamRelNode;
  }

  private Planner getPlanner() {
    return Frameworks.getPlanner(config);
  }
}
