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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.CoreRules;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Statistic;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Statistics;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.Planner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.Programs;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test ensures that we are reordering joins and get a plan similar to Join(large,Join(small,
 * medium)) instead of Join(small, Join(medium,large).
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class JoinReorderingTest {
  private final PipelineOptions defaultPipelineOptions = PipelineOptionsFactory.create();

  @Test
  public void testTableSizes() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);

    Assert.assertEquals(
        1d,
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("small_table"))
            .getTableStatistics(null)
            .getRowCount(),
        0.01);

    Assert.assertEquals(
        3d,
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("medium_table"))
            .getTableStatistics(null)
            .getRowCount(),
        0.01);

    Assert.assertEquals(
        100d,
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("large_table"))
            .getTableStatistics(null)
            .getRowCount(),
        0.01);
  }

  @Test
  public void testBeamJoinAssociationRule() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            CoreRules.SORT_PROJECT_TRANSPOSE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

    String sqlQuery =
        "select * from \"tt\".\"large_table\" as large_table "
            + " JOIN \"tt\".\"medium_table\" as medium_table on large_table.\"medium_key\" = medium_table.\"large_key\" "
            + " JOIN \"tt\".\"small_table\" as small_table on medium_table.\"small_key\" = small_table.\"medium_key\" ";

    RelNode originalPlan = transform(sqlQuery, prepareRules);
    RelNode optimizedPlan =
        transform(
            sqlQuery,
            RuleSets.ofList(
                ImmutableList.<RelOptRule>builder()
                    .addAll(prepareRules)
                    .add(BeamJoinAssociateRule.INSTANCE)
                    .build()));

    assertTopTableInJoins(originalPlan, "small_table");
    assertTopTableInJoins(optimizedPlan, "large_table");
  }

  @Test
  public void testBeamJoinPushThroughJoinRuleLeft() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            CoreRules.SORT_PROJECT_TRANSPOSE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

    String sqlQuery =
        "select * from \"tt\".\"large_table\" as large_table "
            + " JOIN \"tt\".\"medium_table\" as medium_table on large_table.\"medium_key\" = medium_table.\"large_key\" "
            + " JOIN \"tt\".\"small_table\" as small_table on medium_table.\"small_key\" = small_table.\"medium_key\" ";

    RelNode originalPlan = transform(sqlQuery, prepareRules);
    RelNode optimizedPlan =
        transform(
            sqlQuery,
            RuleSets.ofList(
                ImmutableList.<RelOptRule>builder()
                    .addAll(prepareRules)
                    .add(BeamJoinPushThroughJoinRule.LEFT)
                    .build()));

    assertTopTableInJoins(originalPlan, "small_table");
    assertTopTableInJoins(optimizedPlan, "large_table");
  }

  @Test
  public void testBeamJoinPushThroughJoinRuleRight() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            CoreRules.SORT_PROJECT_TRANSPOSE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

    String sqlQuery =
        "select * from \"tt\".\"medium_table\" as medium_table "
            + " JOIN \"tt\".\"large_table\" as large_table on large_table.\"medium_key\" = medium_table.\"large_key\" "
            + " JOIN \"tt\".\"small_table\" as small_table on medium_table.\"small_key\" = small_table.\"medium_key\" ";

    RelNode originalPlan = transform(sqlQuery, prepareRules);
    RelNode optimizedPlan =
        transform(
            sqlQuery,
            RuleSets.ofList(
                ImmutableList.<RelOptRule>builder()
                    .addAll(prepareRules)
                    .add(BeamJoinPushThroughJoinRule.RIGHT)
                    .build()));

    assertTopTableInJoins(originalPlan, "small_table");
    assertTopTableInJoins(optimizedPlan, "large_table");
  }

  @Test
  public void testSystemReorderingLargeMediumSmall() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    // This is Join(Join(large, medium), small) which should be converted to a join that large table
    // is on the top.
    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from large_table "
                + " JOIN medium_table on large_table.medium_key = medium_table.large_key "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key ");
    assertTopTableInJoins(parsedQuery, "large_table");
  }

  @Test
  public void testSystemReorderingMediumLargeSmall() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    // This is Join(Join(medium, large), small) which should be converted to a join that large table
    // is on the top.
    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from medium_table "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key ");
    assertTopTableInJoins(parsedQuery, "large_table");
  }

  @Test
  public void testSystemNotReorderingWithoutRules() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);
    List<RelOptRule> ruleSet =
        BeamRuleSets.getRuleSets().stream()
            .flatMap(rules -> StreamSupport.stream(rules.spliterator(), false))
            .filter(rule -> !(rule instanceof BeamJoinPushThroughJoinRule))
            .filter(rule -> !(rule instanceof BeamJoinAssociateRule))
            .filter(rule -> !(rule instanceof JoinCommuteRule))
            .collect(Collectors.toList());

    BeamSqlEnv env =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(ImmutableList.of(RuleSets.ofList(ruleSet)))
            .build();

    // This is Join(Join(medium, large), small) which should be converted to a join that large table
    // is on the top.
    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from medium_table "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key ");
    assertTopTableInJoins(parsedQuery, "small_table");
  }

  @Test
  public void testSystemNotReorderingMediumSmallLarge() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    // This is a correct ordered join because large table is on the top. It should not change that.
    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from medium_table "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key ");
    assertTopTableInJoins(parsedQuery, "large_table");
  }

  @Test
  public void testSystemNotReorderingSmallMediumLarge() {
    TestTableProvider tableProvider = new TestTableProvider();
    createThreeTables(tableProvider);
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    // This is a correct ordered join because large table is on the top. It should not change that.
    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from small_table "
                + " JOIN medium_table on medium_table.small_key = small_table.medium_key "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key ");
    assertTopTableInJoins(parsedQuery, "large_table");
  }

  private RelNode transform(String sql, RuleSet prepareRules) throws Exception {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defSchema = rootSchema.add("tt", new ThreeTablesSchema());
    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(defSchema)
            .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
            .programs(Programs.of(prepareRules))
            .build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelRoot planRoot = planner.rel(validate);
    RelNode planBefore = planRoot.rel;
    RelTraitSet desiredTraits = planBefore.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return planner.transform(0, desiredTraits, planBefore);
  }

  private void assertTopTableInJoins(RelNode parsedQuery, String expectedTableName) {
    RelNode firstJoin = parsedQuery;
    while (!(firstJoin instanceof Join)) {
      firstJoin = firstJoin.getInput(0);
    }

    RelNode topRight = ((Join) firstJoin).getRight();
    while (!(topRight instanceof Join) && !(topRight instanceof TableScan)) {
      topRight = topRight.getInput(0);
    }

    if (topRight instanceof TableScan) {
      Assert.assertTrue(topRight.getDescription().contains(expectedTableName));
    } else {
      RelNode topLeft = ((Join) firstJoin).getLeft();
      while (!(topLeft instanceof TableScan)) {
        topLeft = topLeft.getInput(0);
      }

      Assert.assertTrue(topLeft.getDescription().contains(expectedTableName));
    }
  }

  private void createThreeTables(TestTableProvider tableProvider) {
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);
    env.executeDdl("CREATE EXTERNAL TABLE small_table (id INTEGER, medium_key INTEGER) TYPE text");

    env.executeDdl(
        "CREATE EXTERNAL TABLE medium_table ("
            + "id INTEGER,"
            + "small_key INTEGER,"
            + "large_key INTEGER"
            + ") TYPE text");

    env.executeDdl(
        "CREATE EXTERNAL TABLE large_table ("
            + "id INTEGER,"
            + "medium_key INTEGER"
            + ") TYPE text");

    Row row =
        Row.withSchema(tableProvider.getTable("small_table").getSchema()).addValues(1, 1).build();
    tableProvider.addRows("small_table", row);

    for (int i = 0; i < 3; i++) {
      row =
          Row.withSchema(tableProvider.getTable("medium_table").getSchema())
              .addValues(i, 1, 2)
              .build();
      tableProvider.addRows("medium_table", row);
    }

    for (int i = 0; i < 100; i++) {
      row =
          Row.withSchema(tableProvider.getTable("large_table").getSchema()).addValues(i, 2).build();
      tableProvider.addRows("large_table", row);
    }
  }
}

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
final class ThreeTablesSchema extends AbstractSchema {

  private final ImmutableMap<String, Table> tables;

  public ThreeTablesSchema() {
    super();
    ArrayList<Object[]> mediumData = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      mediumData.add(new Object[] {i, 1, 2});
    }

    ArrayList<Object[]> largeData = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      largeData.add(new Object[] {i, 2});
    }

    tables =
        ImmutableMap.<String, Table>builder()
            .put(
                "small_table",
                new PkClusteredTable(
                    factory ->
                        new RelDataTypeFactory.Builder(factory)
                            .add("id", factory.createJavaType(int.class))
                            .add("medium_key", factory.createJavaType(int.class))
                            .build(),
                    ImmutableBitSet.of(0),
                    Arrays.asList(new Object[] {1, 1}, new Object[] {2, 1})))
            .put(
                "medium_table",
                new PkClusteredTable(
                    factory ->
                        new RelDataTypeFactory.Builder(factory)
                            .add("id", factory.createJavaType(int.class))
                            .add("small_key", factory.createJavaType(int.class))
                            .add("large_key", factory.createJavaType(int.class))
                            .build(),
                    ImmutableBitSet.of(0),
                    mediumData))
            .put(
                "large_table",
                new PkClusteredTable(
                    factory ->
                        new RelDataTypeFactory.Builder(factory)
                            .add("id", factory.createJavaType(int.class))
                            .add("medium_key", factory.createJavaType(int.class))
                            .build(),
                    ImmutableBitSet.of(0),
                    largeData))
            .build();
  }

  @Override
  protected Map<String, org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Table>
      getTableMap() {
    return tables;
  }

  /** A table sorted (ascending direction and nulls last) on the primary key. */
  private static class PkClusteredTable extends AbstractTable implements ScannableTable {
    private final ImmutableBitSet pkColumns;
    private final List<Object[]> data;
    private final java.util.function.Function<RelDataTypeFactory, RelDataType> typeBuilder;

    PkClusteredTable(
        Function<RelDataTypeFactory, RelDataType> dataTypeBuilder,
        ImmutableBitSet pkColumns,
        List<Object[]> data) {
      this.data = data;
      this.typeBuilder = dataTypeBuilder;
      this.pkColumns = pkColumns;
    }

    @Override
    public Statistic getStatistic() {
      List<RelFieldCollation> collationFields = new ArrayList<>();
      for (Integer key : pkColumns) {
        collationFields.add(
            new RelFieldCollation(
                key, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
      }
      return Statistics.of(
          data.size(),
          ImmutableList.of(pkColumns),
          ImmutableList.of(RelCollations.of(collationFields)));
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return typeBuilder.apply(typeFactory);
    }

    @Override
    public Enumerable<Object[]> scan(final DataContext root) {
      return Linq4j.asEnumerable(data);
    }
  }
}
