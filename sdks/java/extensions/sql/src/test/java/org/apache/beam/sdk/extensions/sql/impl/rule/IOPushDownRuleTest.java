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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.alibaba.fastjson.JSON;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IOPushDownRuleTest {
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt32Field("unused1")
          .addInt32Field("id")
          .addStringField("name")
          .addInt32Field("unused2")
          .build();
  private static final List<RelOptRule> defaultRules =
      ImmutableList.of(
          BeamCalcRule.INSTANCE,
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          CalcMergeRule.INSTANCE);
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
            .setRuleSets(ImmutableList.of(RuleSets.ofList(defaultRules)))
            .build();
  }

  @Test
  public void testFindUtilisedInputRefs() {
    String sqlQuery = "select id+10 from TEST where name='one'";
    BeamRelNode basicRel = sqlEnv.parseQuery(sqlQuery);
    assertThat(basicRel, instanceOf(Calc.class));

    Calc calc = (Calc) basicRel;
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
        calc.getProgram().split();
    final ImmutableList<RexNode> projects = projectFilter.left;
    final ImmutableList<RexNode> filters = projectFilter.right;

    Set<String> usedFields = new HashSet<>();
    BeamIOPushDownRule.INSTANCE.findUtilizedInputRefs(
        calc.getProgram().getInputRowType(), projects.get(0), usedFields);
    assertThat(usedFields, containsInAnyOrder("id"));

    BeamIOPushDownRule.INSTANCE.findUtilizedInputRefs(
        calc.getProgram().getInputRowType(), filters.get(0), usedFields);
    assertThat(usedFields, containsInAnyOrder("id", "name"));
  }

  @Test
  public void testReMapRexNodeToNewInputs() {
    String sqlQuery = "select id+10 from TEST where name='one'";
    BeamRelNode basicRel = sqlEnv.parseQuery(sqlQuery);
    assertThat(basicRel, instanceOf(Calc.class));

    Calc calc = (Calc) basicRel;
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
        calc.getProgram().split();
    final ImmutableList<RexNode> projects = projectFilter.left;
    final ImmutableList<RexNode> filters = projectFilter.right;

    List<Integer> mapping = ImmutableList.of(1, 2);

    RexNode newProject =
        BeamIOPushDownRule.INSTANCE.reMapRexNodeToNewInputs(projects.get(0), mapping);
    assertThat(newProject.toString(), equalTo("+($0, 10)"));

    RexNode newFilter =
        BeamIOPushDownRule.INSTANCE.reMapRexNodeToNewInputs(filters.get(0), mapping);
    assertThat(newFilter.toString(), equalTo("=($1, 'one')"));
  }

  @Test
  public void testIsProjectRenameOnlyProgram() {
    List<Pair<Pair<String, Boolean>, Boolean>> tests =
        ImmutableList.of(
            // Selecting fields in a different order is only allowed with project push-down.
            Pair.of(Pair.of("select unused2, name, id from TEST", true), true),
            Pair.of(Pair.of("select unused2, name, id from TEST", false), false),
            Pair.of(Pair.of("select id from TEST", false), true),
            Pair.of(Pair.of("select * from TEST", false), true),
            Pair.of(Pair.of("select id, name from TEST", false), true),
            Pair.of(Pair.of("select id+10 from TEST", false), false),
            // Note that we only care about projects.
            Pair.of(Pair.of("select id from TEST where name='one'", false), true));

    for (Pair<Pair<String, Boolean>, Boolean> test : tests) {
      String sqlQuery = test.left.left;
      boolean projectPushDownSupported = test.left.right;
      boolean expectedAnswer = test.right;
      BeamRelNode basicRel = sqlEnv.parseQuery(sqlQuery);
      assertThat(basicRel, instanceOf(Calc.class));

      Calc calc = (Calc) basicRel;
      assertThat(
          test.toString(),
          BeamIOPushDownRule.INSTANCE.isProjectRenameOnlyProgram(
              calc.getProgram(), projectPushDownSupported),
          equalTo(expectedAnswer));
    }
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
