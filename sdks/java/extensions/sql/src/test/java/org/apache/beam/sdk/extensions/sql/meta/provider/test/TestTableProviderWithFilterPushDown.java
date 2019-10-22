package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
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
    Table table = getTable("TEST", PushDownOptions.BOTH);
    tableProvider.createTable(table);
    tableProvider.addRows(
        table.getName(),
        row(BASIC_SCHEMA, 100, 1, "one", (short) 100),
        row(BASIC_SCHEMA, 200, 2, "two", (short) 200));

    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(new RuleSet[] {RuleSets.ofList(rulesWithPushDown)})
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
  public void testIOSourceRel_predicateWithAnd() {
    String selectTableStatement = "SELECT name FROM TEST where id=2 and unused1=200";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(Schema.builder().addStringField("name").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectFieldsInRandomOrder_withRename() {
    String selectTableStatement = "SELECT name as new_name, id as new_id FROM TEST where id=2";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(Schema.builder().addStringField("new_name").addInt32Field("new_id").build(), result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), "two", 2));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testIOSourceRel_selectAllField() {
    String selectTableStatement = "SELECT * FROM TEST where id=1";

    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    assertThat(beamRelNode, instanceOf(BeamIOSourceRel.class));
    assertEquals(BASIC_SCHEMA, result.getSchema());
    PAssert.that(result).containsInAnyOrder(row(result.getSchema(), 100, 1, "one", (short) 100));

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
