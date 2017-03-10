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
package org.beam.sdk.java.sql.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.beam.sdk.java.sql.rel.BeamLogicalConvention;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.beam.sdk.java.sql.schema.BaseBeamTable;

/**
 * The core component to handle through a SQL statement, to a Beam pipeline.
 *
 */
public class BeamQueryPlanner {

  public static final int BEAM_REL_CONVERSION_RULES = 1;

  private final Planner planner;
  private Map<String, BaseBeamTable> kafkaTables = new HashMap<>();

  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  public BeamQueryPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
    sqlOperatorTables.add(SqlStdOperatorTable.instance());
    sqlOperatorTables.add(new CalciteCatalogReader(CalciteSchema.from(schema), false,
        Collections.<String>emptyList(), typeFactory));

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            .setLex(Lex.MYSQL).build())
        .defaultSchema(schema)
        .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT).ruleSets(BeamRuleSets.getRuleSets())
        .costFactory(null).typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM).build();
    this.planner = Frameworks.getPlanner(config);

    for (String t : schema.getTableNames()) {
      kafkaTables.put(t, (BaseBeamTable) schema.getTable(t));
    }
  }

  public void compileAndRun(String query) throws Exception {
    BeamRelNode relNode = getPlan(query);

    BeamPipelineCreator planCreator = new BeamPipelineCreator(kafkaTables);

    String beamPlan = RelOptUtil.toString(relNode);
    System.out.println("beamPlan>");
    System.out.println(beamPlan);

    relNode.buildBeamPipeline(planCreator);

    planCreator.runJob();
  }

  public BeamRelNode getPlan(String query)
      throws ValidationException, RelConversionException, SqlParseException {
    return (BeamRelNode) validateAndConvert(planner.parse(query));
  }

  private RelNode validateAndConvert(SqlNode sqlNode)
      throws ValidationException, RelConversionException {
    SqlNode validated = validateNode(sqlNode);
    RelNode relNode = convertToRelNode(validated);
    return convertToBeamRel(relNode);
  }

  private RelNode convertToBeamRel(RelNode relNode) throws RelConversionException {
    RelTraitSet traitSet = relNode.getTraitSet();
    // traitSet = traitSet.simplify();

    System.out.println("SQLPlan>\n" + RelOptUtil.toString(relNode));

    // PlannerImpl.transform() optimizes RelNode with ruleset
    return planner.transform(0, traitSet.replace(BeamLogicalConvention.INSTANCE), relNode);
  }

  private RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
    return planner.rel(sqlNode).rel;
  }

  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedSqlNode = planner.validate(sqlNode);
    validatedSqlNode.accept(new UnsupportedOperatorsVisitor());
    return validatedSqlNode;
  }

}
