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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTableProviderWithFilterPushDown {
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
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          BeamIOPushDownRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          CalcMergeRule.INSTANCE);
  private BeamSqlEnv sqlEnv;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table table = getTable("TEST", PushDownOptions.FILTER);
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
  public void testIOSourceRel_withFilter_shouldProjectAllFields() {
    String selectTableStatement = "SELECT name FROM TEST where name='two'";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    // Condition should be pushed-down to IO level
    assertNull(((Calc) beamRelNode).getProgram().getCondition());

    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    List<String> projects = beamRelNode.getInput(0).getRowType().getFieldNames();
    // When performing standalone filter push-down IO should project all fields.
    assertThat(projects, containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAll_withSupportedFilter_shouldDropCalc() {
    String selectTableStatement = "SELECT * FROM TEST where name='two'";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    // Calc is dropped, because all fields are projected in the same order and filter is
    // pushed-down.
    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));

    List<String> projects = beamRelNode.getRowType().getFieldNames();
    assertThat(projects, containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

    assertEquals(BASIC_SCHEMA, result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 200, 2, "two", (short) 200, false));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withSupportedFilter_selectInRandomOrder() {
    String selectTableStatement = "SELECT unused2, id, name FROM TEST where b";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    // Condition should be pushed-down to IO level
    assertNull(((Calc) beamRelNode).getProgram().getCondition());

    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    List<String> projects = beamRelNode.getInput(0).getRowType().getFieldNames();
    // When performing standalone filter push-down IO should project all fields.
    assertThat(projects, containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

    assertEquals(
        Schema.builder()
            .addInt16Field("unused2")
            .addInt32Field("id")
            .addStringField("name")
            .build(),
        result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), (short) 100, 1, "one"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_withUnsupportedFilter_calcPreservesCondition() {
    String selectTableStatement = "SELECT name FROM TEST where id+1=2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    // Unsupported condition should be preserved in a Calc
    assertNotNull(((Calc) beamRelNode).getProgram().getCondition());

    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    List<String> projects = beamRelNode.getInput(0).getRowType().getFieldNames();
    // When performing standalone filter push-down IO should project all fields.
    assertThat(projects, containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "one"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAllFieldsInRandomOrder_shouldPushDownSupportedFilter() {
    String selectTableStatement = "SELECT unused2, name, id, b, unused1 FROM TEST where name='two'";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    // Calc should not be dropped, because fields are selected in a different order, even though
    //  all filters are supported and all fields are projected.
    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    assertNull(((BeamCalcRel) beamRelNode).getProgram().getCondition());

    assertThat(beamRelNode.getInput(0), instanceOf(BeamIOSourceRel.class));
    List<String> projects = beamRelNode.getInput(0).getRowType().getFieldNames();
    // When performing standalone filter push-down IO should project all fields.
    assertThat(projects, containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

    assertEquals(
        Schema.builder()
            .addInt16Field("unused2")
            .addStringField("name")
            .addInt32Field("id")
            .addBooleanField("b")
            .addInt32Field("unused1")
            .build(),
        result.getSchema());
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), (short) 200, "two", 2, false, 200));

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
    // When performing standalone filter push-down IO should project all fields.
    assertThat(
        pushedFields,
        IsIterableContainingInAnyOrder.containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

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
    assertThat(
        pushedFields,
        IsIterableContainingInAnyOrder.containsInAnyOrder("unused1", "id", "name", "unused2", "b"));

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

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }
}
