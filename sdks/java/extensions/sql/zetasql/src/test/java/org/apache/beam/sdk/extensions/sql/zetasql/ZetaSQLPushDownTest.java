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

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Context;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Contexts;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZetaSQLPushDownTest {
  private static final Long PIPELINE_EXECUTION_WAITTIME_MINUTES = 2L;
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt64Field("unused1")
          .addInt64Field("id")
          .addStringField("name")
          .addInt64Field("unused2")
          .build();

  // These fields are being initialized in Setup method
  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static TestTableProvider tableProvider;
  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static FrameworkConfig config;
  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static ZetaSQLQueryPlanner zetaSQLQueryPlanner;
  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static BeamSqlEnv sqlEnv;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    initializeBeamTableProvider();
    initializeCalciteEnvironment();
    zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
  }

  @Test
  public void testProjectPushDown_withoutPredicate() {
    String sql = "SELECT name, id, unused1 FROM InMemoryTableProject";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode, instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getDigest(), zetaSqlNode.getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectPushDown_withoutPredicate_withComplexSelect() {
    String sql = "SELECT id+1 FROM InMemoryTableProject";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getInput(0).getDigest(), zetaSqlNode.getInput(0).getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectPushDown_withPredicate() {
    String sql = "SELECT name FROM InMemoryTableProject where id=2";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getInput(0).getDigest(), zetaSqlNode.getInput(0).getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectFilterPushDown_withoutPredicate() {
    String sql = "SELECT name, id, unused1 FROM InMemoryTableBoth";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode, instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getDigest(), zetaSqlNode.getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectFilterPushDown_withSupportedPredicate() {
    String sql = "SELECT name FROM InMemoryTableBoth where id=2";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode, instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getDigest(), zetaSqlNode.getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectFilterPushDown_withUnsupportedPredicate() {
    String sql = "SELECT name FROM InMemoryTableBoth where id=2 or unused1=200";

    BeamRelNode zetaSqlNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamRelNode calciteSqlNode = sqlEnv.parseQuery(sql);

    assertThat(zetaSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertThat(calciteSqlNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(calciteSqlNode.getInput(0).getDigest(), zetaSqlNode.getInput(0).getDigest());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  private static void initializeCalciteEnvironment() {
    initializeCalciteEnvironmentWithContext();
  }

  private static void initializeCalciteEnvironmentWithContext(Context... extraContext) {
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());
    SchemaPlus defaultSchemaPlus = jdbcConnection.getCurrentSchemaPlus();
    final ImmutableList<RelTraitDef> traitDefs = ImmutableList.of(ConventionTraitDef.INSTANCE);

    Object[] contexts =
        ImmutableList.<Context>builder()
            .add(Contexts.of(jdbcConnection.config()))
            .add(extraContext)
            .build()
            .toArray();

    config =
        Frameworks.newConfigBuilder()
            .defaultSchema(defaultSchemaPlus)
            .traitDefs(traitDefs)
            .context(Contexts.of(contexts))
            .ruleSets(ZetaSQLQueryPlanner.getZetaSqlRuleSets().toArray(new RuleSet[0]))
            .costFactory(BeamCostModel.FACTORY)
            .typeSystem(jdbcConnection.getTypeFactory().getTypeSystem())
            .build();
  }

  private static void initializeBeamTableProvider() {
    Table projectTable = getTable("InMemoryTableProject", PushDownOptions.PROJECT);
    Table bothTable = getTable("InMemoryTableBoth", PushDownOptions.BOTH);
    Row[] rows = {
      row(BASIC_SCHEMA, 100L, 1L, "one", 100L), row(BASIC_SCHEMA, 200L, 2L, "two", 200L)
    };

    tableProvider = new TestTableProvider();
    tableProvider.createTable(projectTable);
    tableProvider.createTable(bothTable);
    tableProvider.addRows(projectTable.getName(), rows);
    tableProvider.addRows(bothTable.getName(), rows);
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }

  private static Table getTable(String name, PushDownOptions options) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .schema(BASIC_SCHEMA)
        .properties(
            JSON.parseObject("{ " + PUSH_DOWN_OPTION + ": " + "\"" + options.toString() + "\" }"))
        .type("test")
        .build();
  }
}
