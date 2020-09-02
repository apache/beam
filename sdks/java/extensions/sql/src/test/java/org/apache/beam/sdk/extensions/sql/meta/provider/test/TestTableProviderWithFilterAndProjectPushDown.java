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
import static org.junit.Assert.assertNull;

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
public class TestTableProviderWithFilterAndProjectPushDown {
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt32Field("unused1")
          .addInt32Field("id")
          .addStringField("name")
          .addInt16Field("unused2")
          .addBooleanField("b")
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
    Table table = getTable("TEST", PushDownOptions.BOTH);
    tableProvider.createTable(table);
    tableProvider.addRows(
        table.getName(),
        row(BASIC_SCHEMA, 100, 1, "one", (short) 100, true),
        row(BASIC_SCHEMA, 200, 2, "two", (short) 200, false));

    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(ImmutableList.of(RuleSets.ofList(rulesWithPushDown)))
            .build();
  }

  @Test
  public void testIOSourceRel_predicateSimple() {
    String selectTableStatement = "SELECT name FROM TEST where id=2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_predicateSimple_Boolean() {
    String selectTableStatement = "SELECT name FROM TEST where b";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "one"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_predicateWithAnd() {
    String selectTableStatement = "SELECT name FROM TEST where id>=2 and unused1<=200";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withComplexProjects_withSupportedFilter() {
    String selectTableStatement =
        "SELECT name as new_name, unused1+10-id as new_id FROM TEST where 1<id";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "unused1", "id"));
    assertEquals(
        Schema.builder().addStringField("new_name").addInt32Field("new_id").build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two", 208));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectFieldsInRandomOrder_withRename_withSupportedFilter() {
    String selectTableStatement =
        "SELECT name as new_name, id as new_id, unused1 as new_unused1 FROM TEST where 1<id";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> a = beamRelNode.getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("new_name", "new_id", "new_unused1"));
    assertEquals(
        Schema.builder()
            .addStringField("new_name")
            .addInt32Field("new_id")
            .addInt32Field("new_unused1")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two", 2, 200));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectFieldsInRandomOrder_withRename_withUnsupportedFilter() {
    String selectTableStatement =
        "SELECT name as new_name, id as new_id, unused1 as new_unused1 FROM TEST where id+unused1=202";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "id", "unused1"));
    assertEquals(
        Schema.builder()
            .addStringField("new_name")
            .addInt32Field("new_id")
            .addInt32Field("new_unused1")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two", 2, 200));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void
      testIOSourceRel_selectFieldsInRandomOrder_withRename_withSupportedAndUnsupportedFilters() {
    String selectTableStatement =
        "SELECT name as new_name, id as new_id, unused1 as new_unused1 FROM TEST where 1<id and id+unused1=202";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "id", "unused1"));
    assertEquals(
        "BeamPushDownIOSourceRel.BEAM_LOGICAL(table=[beam, TEST],usedFields=[name, id, unused1],TestTableFilter=[supported{<(1, $1)}, unsupported{=(+($1, $0), 202)}])",
        beamRelNode.getInput(0).getDigest());
    assertEquals(
        Schema.builder()
            .addStringField("new_name")
            .addInt32Field("new_id")
            .addInt32Field("new_unused1")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two", 2, 200));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAllField() {
    String selectTableStatement = "SELECT * FROM TEST where id<>2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(
        "BeamPushDownIOSourceRel.BEAM_LOGICAL(table=[beam, TEST],usedFields=[unused1, id, name, unused2, b],TestTableFilter=[supported{<>($1, 2)}, unsupported{}])",
        beamRelNode.getDigest());
    assertEquals(BASIC_SCHEMA, result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 100, 1, "one", (short) 100, true));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }

  @Test
  public void testIOSourceRel_withUnsupportedPredicate() {
    String selectTableStatement = "SELECT name FROM TEST where id+unused1=101";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(
        "BeamPushDownIOSourceRel.BEAM_LOGICAL(table=[beam, TEST],usedFields=[name, id, unused1],TestTableFilter=[supported{}, unsupported{=(+($1, $0), 101)}])",
        beamRelNode.getInput(0).getDigest());
    // Make sure project push-down was done
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "id", "unused1"));

    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "one"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAll_withUnsupportedPredicate() {
    String selectTableStatement = "SELECT * FROM TEST where id+unused1=101";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(
        "BeamIOSourceRel.BEAM_LOGICAL(table=[beam, TEST])", beamRelNode.getInput(0).getDigest());
    // Make sure project push-down was done (all fields since 'select *')
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "id", "unused1", "unused2", "b"));

    assertEquals(BASIC_SCHEMA, result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 100, 1, "one", (short) 100, true));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withSupportedAndUnsupportedPredicate() {
    String selectTableStatement = "SELECT name FROM TEST where id+unused1=101 and id=1";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(
        "BeamPushDownIOSourceRel.BEAM_LOGICAL(table=[beam, TEST],usedFields=[name, id, unused1],TestTableFilter=[supported{=($1, 1)}, unsupported{=(+($1, $0), 101)}])",
        beamRelNode.getInput(0).getDigest());
    // Make sure project push-down was done
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("name", "id", "unused1"));

    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "one"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAll_withSupportedAndUnsupportedPredicate() {
    String selectTableStatement = "SELECT * FROM TEST where id+unused1=101 and id=1";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    assertEquals(
        "BeamPushDownIOSourceRel.BEAM_LOGICAL(table=[beam, TEST],usedFields=[unused1, id, name, unused2, b],TestTableFilter=[supported{=($1, 1)}, unsupported{=(+($1, $0), 101)}])",
        beamRelNode.getInput(0).getDigest());
    // Make sure project push-down was done (all fields since 'select *')
    List<String> a = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(a, containsInAnyOrder("unused1", "name", "id", "unused2", "b"));

    assertEquals(BASIC_SCHEMA, result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 100, 1, "one", (short) 100, true));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectOneFieldsMoreThanOnce() {
    String selectTableStatement = "SELECT b, b, b, b, b FROM TEST";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    // Calc must not be dropped
    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> pushedFields = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(pushedFields, containsInAnyOrder("b"));

    assertEquals(
        Schema.builder()
            .addBooleanField("b")
            .addBooleanField("b0")
            .addBooleanField("b1")
            .addBooleanField("b2")
            .addBooleanField("b3")
            .build(),
        result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(
            row(result.getSchema(), true, true, true, true, true),
            row(result.getSchema(), false, false, false, false, false));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectOneFieldsMoreThanOnce_withSupportedPredicate() {
    String selectTableStatement = "SELECT b, b, b, b, b FROM TEST where b";

    // Calc must not be dropped
    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    // Supported predicate should be pushed-down
    assertNull(((BeamCalcRel) beamRelNode).getProgram().getCondition());
    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    // Make sure project push-down was done
    List<String> pushedFields = beamRelNode.getInput(0).getRowType().getFieldNames();
    assertThat(pushedFields, containsInAnyOrder("b"));

    assertEquals(
        Schema.builder()
            .addBooleanField("b")
            .addBooleanField("b0")
            .addBooleanField("b1")
            .addBooleanField("b2")
            .addBooleanField("b3")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), true, true, true, true, true));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
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
