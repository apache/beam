package org.apache.beam.sdk.extensions.sql.impl.rule;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamAggregationRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BeamAggregateProjectMergeRuleTest {
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt32Field("unused1")
          .addInt32Field("id")
          .addStringField("name")
          .addInt32Field("unused2")
          .build();
  private BeamSqlEnv sqlEnv;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table projectTable = getTable("TEST_PROJECT", PushDownOptions.PROJECT);
    Table filterTable = getTable("TEST_FILTER", PushDownOptions.FILTER);
    Table noneTable = getTable("TEST_NONE", PushDownOptions.NONE);
    tableProvider.createTable(projectTable);
    tableProvider.createTable(filterTable);
    tableProvider.createTable(noneTable);
    sqlEnv = BeamSqlEnv.inMemory(tableProvider);
  }


  @Test
  public void testBeamAggregateProjectMergeRule_withProjectTable() {
    // When an IO supports project push-down, Projects should be merged with an IO.
    String sqlQuery = "select SUM(id) as id_sum from TEST_PROJECT group by name";
    BeamRelNode beamRel = sqlEnv.parseQuery(sqlQuery);

    BeamAggregationRel aggregate = (BeamAggregationRel) beamRel.getInput(0);
    BeamIOSourceRel ioSourceRel = (BeamIOSourceRel) aggregate.getInput();

    // Make sure project push-down took place.
    assertThat(ioSourceRel, instanceOf(BeamPushDownIOSourceRel.class));
    assertThat(ioSourceRel.getRowType().getFieldNames(), containsInAnyOrder("name", "id"));
  }

  @Test
  public void testBeamAggregateProjectMergeRule_withFilterTable() {
    // When an IO does not supports project push-down, Projects should be merged with an aggregate.
    String sqlQuery = "select SUM(id) as id_sum from TEST_FILTER group by name";
    BeamRelNode beamRel = sqlEnv.parseQuery(sqlQuery);

    BeamAggregationRel aggregate = (BeamAggregationRel) beamRel.getInput(0);
    BeamIOSourceRel ioSourceRel = (BeamIOSourceRel) aggregate.getInput();

    // Make sure project merged with an aggregate.
    assertThat(aggregate.getRowType().getFieldNames(), containsInAnyOrder("id_sum", "name"));

    // IO projects al fields.
    assertThat(ioSourceRel, instanceOf(BeamIOSourceRel.class));
    assertThat(ioSourceRel.getRowType().getFieldNames(), containsInAnyOrder("unused1", "name", "id", "unused2"));
  }

  @Test
  public void testBeamAggregateProjectMergeRule_withNoneTable() {
    // When an IO does not supports project push-down, Projects should be merged with an aggregate.
    String sqlQuery = "select SUM(id) as id_sum from TEST_NONE group by name";
    BeamRelNode beamRel = sqlEnv.parseQuery(sqlQuery);

    BeamAggregationRel aggregate = (BeamAggregationRel) beamRel.getInput(0);
    BeamIOSourceRel ioSourceRel = (BeamIOSourceRel) aggregate.getInput();

    // Make sure project merged with an aggregate.
    assertThat(aggregate.getRowType().getFieldNames(), containsInAnyOrder("id_sum", "name"));

    // IO projects al fields.
    assertThat(ioSourceRel, instanceOf(BeamIOSourceRel.class));
    assertThat(ioSourceRel.getRowType().getFieldNames(), containsInAnyOrder("unused1", "name", "id", "unused2"));
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
