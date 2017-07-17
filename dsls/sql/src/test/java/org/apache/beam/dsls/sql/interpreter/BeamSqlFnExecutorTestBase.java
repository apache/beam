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
package org.apache.beam.dsls.sql.interpreter;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.dsls.sql.planner.BeamRelDataTypeSystem;
import org.apache.beam.dsls.sql.planner.BeamRuleSets;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowType;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.BeforeClass;

/**
 * base class to test {@link BeamSqlFnExecutor} and subclasses of {@link BeamSqlExpression}.
 */
public class BeamSqlFnExecutorTestBase {
  public static RexBuilder rexBuilder = new RexBuilder(BeamQueryPlanner.TYPE_FACTORY);
  public static RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

  public static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);
  public static RelDataType relDataType;

  public static BeamSqlRowType beamRecordType;
  public static BeamSqlRow record;

  public static RelBuilder relBuilder;

  @BeforeClass
  public static void prepare() {
    relDataType = TYPE_FACTORY.builder()
        .add("order_id", SqlTypeName.BIGINT)
        .add("site_id", SqlTypeName.INTEGER)
        .add("price", SqlTypeName.DOUBLE)
        .add("order_time", SqlTypeName.BIGINT).build();

    beamRecordType = CalciteUtils.toBeamRowType(relDataType);
    record = new BeamSqlRow(beamRecordType);

    record.addField(0, 1234567L);
    record.addField(1, 0);
    record.addField(2, 8.9);
    record.addField(3, 1234567L);

    SchemaPlus schema = Frameworks.createRootSchema(true);
    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build()).defaultSchema(schema)
        .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT).ruleSets(BeamRuleSets.getRuleSets())
        .costFactory(null).typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM).build();

    relBuilder = RelBuilder.create(config);
  }
}
