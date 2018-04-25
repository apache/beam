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
package org.apache.beam.sdk.extensions.sql.impl.interpreter;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.values.Row;
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

/** base class to test {@link BeamSqlFnExecutor} and subclasses of {@link BeamSqlExpression}. */
public class BeamSqlFnExecutorTestBase {
  static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  static RexBuilder rexBuilder = new RexBuilder(BeamQueryPlanner.TYPE_FACTORY);
  static RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
  static RelDataType relDataType;
  static RelBuilder relBuilder;

  public static Row row;

  @BeforeClass
  public static void prepare() {
    relDataType =
        TYPE_FACTORY
            .builder()
            .add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE)
            .add("order_time", SqlTypeName.BIGINT)
            .build();

    row =
        Row.withSchema(CalciteUtils.toBeamSchema(relDataType))
            .addValues(1234567L, 0, 8.9, 1234567L)
            .build();

    BeamSqlEnv sqlEnv = new BeamSqlEnv();
    SchemaPlus schema = Frameworks.createRootSchema(true);
    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
            .defaultSchema(schema)
            .traitDefs(traitDefs)
            .context(Contexts.EMPTY_CONTEXT)
            .ruleSets(BeamRuleSets.getRuleSets(sqlEnv))
            .costFactory(null)
            .typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM)
            .build();

    relBuilder = RelBuilder.create(config);
  }
}
