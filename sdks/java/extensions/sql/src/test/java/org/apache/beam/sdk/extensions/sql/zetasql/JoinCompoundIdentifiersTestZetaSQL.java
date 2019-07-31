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

import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.BASIC_TABLE_ONE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.BASIC_TABLE_TWO;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_STRUCT;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for identifiers. */
@RunWith(JUnit4.class)
public class JoinCompoundIdentifiersTestZetaSQL {

  private static final Long TWO_MINUTES = 2L;
  private static final String DEFAULT_SCHEMA = "beam";
  private static final String FULL_ON_ID =
      "a.`b-\\`c`.d.`httz://d.e-f.g:233333/blah\\?yes=1&true=false`";
  private static final String TABLE_WITH_STRUCTS_ID = "a.`table:com`.`..::with-struct::..`";

  private static final TableProvider TEST_TABLES =
      new ReadOnlyTableProvider(
          "test_table_provider",
          ImmutableMap.<String, BeamSqlTable>builder()
              .put("KeyValue", BASIC_TABLE_ONE)
              .put("a.b", BASIC_TABLE_ONE)
              .put("c.d.e", BASIC_TABLE_ONE)
              .put("c.d.f", BASIC_TABLE_TWO)
              .put("c.g.e", BASIC_TABLE_TWO)
              .put("weird.`\\n\\t\\r\\f`", BASIC_TABLE_ONE)
              .put("a.`b-\\`c`.d", BASIC_TABLE_TWO)
              .put(FULL_ON_ID, BASIC_TABLE_TWO)
              .put(TABLE_WITH_STRUCTS_ID, TABLE_WITH_STRUCT)
              .build());

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testComplexTableName() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT Key FROM a.b");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testComplexTableName3Levels() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT Key FROM c.d.e");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testOnePartWithBackticks() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT RowKey FROM a.`b-\\`c`.d");

    PAssert.that(result).containsInAnyOrder(singleValue(16L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testNewLinesAndOtherWhitespace() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(pipeline, cfg, "SELECT Key FROM weird.`\\n\\t\\r\\f`");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testFullOnWithBackticks() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT RowKey FROM " + FULL_ON_ID);

    PAssert.that(result).containsInAnyOrder(singleValue(16L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testJoinWithFullOnWithBackticks() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT t1.RowKey FROM "
                + FULL_ON_ID
                + " AS t1 \n"
                + " INNER JOIN a.`b-\\`c`.d t2 on t1.RowKey = t2.RowKey");

    PAssert.that(result).containsInAnyOrder(singleValue(16L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithAliasedComplexTableName() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT t.Key FROM a.b AS t");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithAliasedComplexTableName3Levels() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT t.Key FROM c.d.e AS t");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithUnaliasedComplexTableName() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT b.Key FROM a.b");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithUnaliasedComplexTableName3Levels() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT e.Key FROM c.d.e");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithUnaliasedComplexTableName3Levels2() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result = applySqlTransform(pipeline, cfg, "SELECT e.Key FROM c.d.e");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithJoinOfAliasedComplexTableNames() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT t1.Key FROM a.b AS t1 INNER JOIN c.d.e AS t2 ON t1.Key = t2.Key");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testJoinTwoTablesWithLastPartIdDifferent() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT t1.Key FROM c.d.e AS t1 INNER JOIN c.d.f AS t2 ON t1.Key = t2.RowKey");

    PAssert.that(result).containsInAnyOrder(singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testJoinTwoTablesWithMiddlePartIdDifferent() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT t1.Key FROM c.d.e AS t1 INNER JOIN c.g.e AS t2 ON t1.Key = t2.RowKey");

    PAssert.that(result).containsInAnyOrder(singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedFieldAccessWithJoinOfUnaliasedComplexTableNames() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(pipeline, cfg, "SELECT b.Key FROM a.b INNER JOIN c.d.e ON b.Key = e.Key");

    PAssert.that(result).containsInAnyOrder(singleValue(14L), singleValue(15L));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testStructFieldAccess() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT struct_col.struct_col_str FROM a.`table:com`.`..::with-struct::..`");

    PAssert.that(result).containsInAnyOrder(singleValue("row_one"), singleValue("row_two"));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testQualifiedStructFieldAccess() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT `..::with-struct::..`.struct_col.struct_col_str \n"
                + " FROM a.`table:com`.`..::with-struct::..`");

    PAssert.that(result).containsInAnyOrder(singleValue("row_one"), singleValue("row_two"));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @Test
  public void testAliasedStructFieldAccess() throws Exception {
    FrameworkConfig cfg = initializeCalcite();

    PCollection<Row> result =
        applySqlTransform(
            pipeline,
            cfg,
            "SELECT t.struct_col.struct_col_str FROM " + TABLE_WITH_STRUCTS_ID + " t");

    PAssert.that(result).containsInAnyOrder(singleValue("row_one"), singleValue("row_two"));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(TWO_MINUTES));
  }

  @SuppressWarnings("unxchecked")
  private static FrameworkConfig initializeCalcite() {
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(TEST_TABLES, PipelineOptionsFactory.create());
    SchemaPlus defaultSchemaPlus = jdbcConnection.getCurrentSchemaPlus();
    List<RelTraitDef> traitDefs = ImmutableList.of(ConventionTraitDef.INSTANCE);

    Object[] contexts =
        ImmutableList.of(
                Contexts.of(jdbcConnection.config()),
                TableResolutionContext.joinCompoundIds(DEFAULT_SCHEMA))
            .toArray();

    return Frameworks.newConfigBuilder()
        .defaultSchema(defaultSchemaPlus)
        .traitDefs(traitDefs)
        .context(Contexts.of(contexts))
        .ruleSets(BeamRuleSets.getRuleSets())
        .costFactory(null)
        .typeSystem(jdbcConnection.getTypeFactory().getTypeSystem())
        .build();
  }

  private PCollection<Row> applySqlTransform(
      Pipeline pipeline, FrameworkConfig config, String query) throws Exception {

    BeamRelNode beamRelNode = new ZetaSQLQueryPlanner(config).parseQuery(query);
    return BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  private Row singleValue(long value) {
    return Row.withSchema(singleLongField()).addValue(value).build();
  }

  private Row singleValue(String value) {
    return Row.withSchema(singleStringField()).addValue(value).build();
  }

  private Schema singleLongField() {
    return Schema.builder().addInt64Field("field1").build();
  }

  private Schema singleStringField() {
    return Schema.builder().addStringField("field1").build();
  }
}
