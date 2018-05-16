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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core component to handle through a SQL statement, from explain execution plan,
 * to generate a Beam pipeline.
 *
 */
public class BeamQueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(BeamQueryPlanner.class);

  private final FrameworkConfig config;

  public static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  public BeamQueryPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
    sqlOperatorTables.add(SqlStdOperatorTable.instance());
    sqlOperatorTables.add(
        new CalciteCatalogReader(
            CalciteSchema.from(schema).root(), Collections.emptyList(), TYPE_FACTORY, null));

    config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                .setLex(Lex.JAVA)
                .setParserFactory(BeamSqlParserImpl.FACTORY)
                .build())
            .defaultSchema(schema)
            .traitDefs(traitDefs)
            .context(Contexts.EMPTY_CONTEXT)
            .ruleSets(BeamRuleSets.getRuleSets())
            .costFactory(null)
            .typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM)
            .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
            .build();
  }

  /**
   * Parse input SQL query, and return a {@link SqlNode} as grammar tree.
   */
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
   * {@code compileBeamPipeline} translate a SQL statement to executed as Beam data flow,
   * which is linked with the given {@code pipeline}. The final output stream is returned as
   * {@code PCollection} so more operations can be applied.
   */
  public PCollection<Row> compileBeamPipeline(String sqlStatement, Pipeline basePipeline)
      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode relNode = convertToBeamRel(sqlStatement);

    // the input PCollectionTuple is empty, and be rebuilt in BeamIOSourceRel.
    return PCollectionTuple.empty(basePipeline).apply(relNode.toPTransform());
  }

  /**
   * It parses and validate the input query, then convert into a
   * {@link BeamRelNode} tree.
   *
   */
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

      RelTraitSet desiredTraits = root.rel.getTraitSet()
          .replace(BeamLogicalConvention.INSTANCE)
          .replace(root.collation)
          .simplify();
      beamRelNode = (BeamRelNode) planner.transform(0, desiredTraits, root.rel);
    } finally {
      planner.close();
    }
    return beamRelNode;
  }

  private Planner getPlanner() {
    return Frameworks.getPlanner(config);
  }

}
