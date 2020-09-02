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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.alibaba.fastjson.JSON;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOPushDownRule;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.CoreRules;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RuleSets;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TestTableProviderWithProjectPushDown {
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt32Field("unused1")
          .addInt32Field("id")
          .addStringField("name")
          .addInt32Field("unused2")
          .build();
  private static final List<RelOptRule> rulesWithPushDown =
      ImmutableList.of(
          BeamCalcRule.INSTANCE,
          CoreRules.FILTER_CALC_MERGE,
          CoreRules.PROJECT_CALC_MERGE,
          BeamIOPushDownRule.INSTANCE,
          CoreRules.FILTER_TO_CALC,
          CoreRules.PROJECT_TO_CALC,
          CoreRules.CALC_MERGE);
  private BeamSqlEnv sqlEnv;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table table = getTable("TEST", PushDownOptions.PROJECT);
    tableProvider.createTable(table);
    tableProvider.addRows(
        table.getName(),
        row(BASIC_SCHEMA, 100, 1, "one", 100),
        row(BASIC_SCHEMA, 200, 2, "two", 200));

    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(ImmutableList.of(RuleSets.ofList(rulesWithPushDown)))
            .build();
  }

  @Test
  public void testIOSourceRel_withNoPredicate() {
    String selectTableStatement = "SELECT id, name FROM TEST";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertEquals(
        result.getSchema(), Schema.builder().addInt32Field("id").addStringField("name").build());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 1, "one"), row(result.getSchema(), 2, "two"));
    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    // If project push-down succeeds new BeamIOSourceRel should not output unused fields
    assertThat(beamRelNode.getRowType().getFieldNames(), containsInAnyOrder("id", "name"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withNoPredicate_withRename() {
    String selectTableStatement = "SELECT id as new_id, name as new_name FROM TEST";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertEquals(
        result.getSchema(),
        Schema.builder().addInt32Field("new_id").addStringField("new_name").build());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 1, "one"), row(result.getSchema(), 2, "two"));
    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    // If project push-down succeeds new BeamIOSourceRel should not output unused fields
    assertThat(beamRelNode.getRowType().getFieldNames(), containsInAnyOrder("new_id", "new_name"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withPredicate() {
    String selectTableStatement = "SELECT name FROM TEST where id=2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertEquals(result.getSchema(), Schema.builder().addStringField("name").build());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // When doing only project push-down, predicate should be preserved in a Calc and IO should
    // project fields queried + fields used by the predicate
    assertThat(
        beamRelNode.getInput(0).getRowType().getFieldNames(), containsInAnyOrder("id", "name"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withPredicate_withRename() {
    String selectTableStatement = "SELECT name as new_name FROM TEST where id=2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertEquals(result.getSchema(), Schema.builder().addStringField("new_name").build());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // When doing only project push-down, predicate (and rename) should be preserved in a Calc
    assertThat(beamRelNode.getRowType().getFieldNames(), containsInAnyOrder("new_name"));
    // IO should project fields queried + fields used by the predicate
    assertThat(
        beamRelNode.getInput(0).getRowType().getFieldNames(), containsInAnyOrder("id", "name"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_AllFields() {
    String selectTableStatement = "SELECT * FROM TEST";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertEquals(result.getSchema(), BASIC_SCHEMA);
    PAssert.that(result)
        .containsInAnyOrder(
            row(result.getSchema(), 100, 1, "one", 100),
            row(result.getSchema(), 200, 2, "two", 200));
    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // If project push-down succeeds new BeamIOSourceRel should not output unused fields
    assertThat(
        beamRelNode.getInput(0).getRowType().getFieldNames(),
        containsInAnyOrder("unused1", "id", "name", "unused2"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectOneFieldsMoreThanOnce() {
    String selectTableStatement = "SELECT id, id, id, id, id FROM TEST";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    // Calc must not be dropped
    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> pushedFields = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(pushedFields, containsInAnyOrder("id"));

    assertEquals(
        Schema.builder()
            .addInt32Field("id")
            .addInt32Field("id0")
            .addInt32Field("id1")
            .addInt32Field("id2")
            .addInt32Field("id3")
            .build(),
        result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(
            row(result.getSchema(), 1, 1, 1, 1, 1), row(result.getSchema(), 2, 2, 2, 2, 2));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectOneFieldsMoreThanOnce_withSupportedPredicate() {
    String selectTableStatement = "SELECT id, id, id, id, id FROM TEST where id=1";

    // Calc must not be dropped
    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    // Project push-down should leave predicate in a Calc
    assertNotNull(((BeamCalcRel) beamRelNode).getProgram().getCondition());
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> pushedFields = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(pushedFields, containsInAnyOrder("id"));

    assertEquals(
        Schema.builder()
            .addInt32Field("id")
            .addInt32Field("id0")
            .addInt32Field("id1")
            .addInt32Field("id2")
            .addInt32Field("id3")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), 1, 1, 1, 1, 1));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
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
