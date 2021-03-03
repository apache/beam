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

import com.google.zetasql.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Contexts;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Rule;

/** Common setup for ZetaSQL tests. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  protected static final Duration PIPELINE_EXECUTION_WAITTIME = Duration.standardMinutes(2L);

  protected FrameworkConfig config;

  protected PCollection<Row> execute(String sql, QueryParameters params) {
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    return BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  protected PCollection<Row> execute(String sql) {
    return execute(sql, QueryParameters.ofNone());
  }

  protected PCollection<Row> execute(String sql, Map<String, Value> params) {
    return execute(sql, QueryParameters.ofNamed(params));
  }

  protected PCollection<Row> execute(String sql, List<Value> params) {
    return execute(sql, QueryParameters.ofPositional(params));
  }

  private TableProvider createBeamTableProvider() {
    Map<String, BeamSqlTable> testBoundedTableMap = new HashMap<>();
    testBoundedTableMap.put("KeyValue", TestInput.BASIC_TABLE_ONE);
    testBoundedTableMap.put("BigTable", TestInput.BASIC_TABLE_TWO);
    testBoundedTableMap.put("Spanner", TestInput.BASIC_TABLE_THREE);
    testBoundedTableMap.put("aggregate_test_table", TestInput.AGGREGATE_TABLE_ONE);
    testBoundedTableMap.put("window_test_table", TestInput.TIMESTAMP_TABLE_ONE);
    testBoundedTableMap.put("window_test_table_two", TestInput.TIMESTAMP_TABLE_TWO);
    testBoundedTableMap.put("all_null_table", TestInput.TABLE_ALL_NULL);
    testBoundedTableMap.put("table_with_struct", TestInput.TABLE_WITH_STRUCT);
    testBoundedTableMap.put("table_with_struct_two", TestInput.TABLE_WITH_STRUCT_TWO);
    testBoundedTableMap.put("table_with_array", TestInput.TABLE_WITH_ARRAY);
    testBoundedTableMap.put("table_with_array_for_unnest", TestInput.TABLE_WITH_ARRAY_FOR_UNNEST);
    testBoundedTableMap.put("table_with_array_of_struct", TestInput.TABLE_WITH_ARRAY_OF_STRUCT);
    testBoundedTableMap.put("table_with_struct_of_struct", TestInput.TABLE_WITH_STRUCT_OF_STRUCT);
    testBoundedTableMap.put(
        "table_with_struct_of_struct_of_array", TestInput.TABLE_WITH_STRUCT_OF_STRUCT_OF_ARRAY);
    testBoundedTableMap.put(
        "table_with_array_of_struct_of_struct", TestInput.TABLE_WITH_ARRAY_OF_STRUCT_OF_STRUCT);
    testBoundedTableMap.put(
        "table_with_struct_of_array_of_struct", TestInput.TABLE_WITH_STRUCT_OF_ARRAY_OF_STRUCT);
    testBoundedTableMap.put("table_for_case_when", TestInput.TABLE_FOR_CASE_WHEN);
    testBoundedTableMap.put("aggregate_test_table_two", TestInput.AGGREGATE_TABLE_TWO);
    testBoundedTableMap.put("table_empty", TestInput.TABLE_EMPTY);
    testBoundedTableMap.put("table_all_types", TestInput.TABLE_ALL_TYPES);
    testBoundedTableMap.put("table_all_types_2", TestInput.TABLE_ALL_TYPES_2);
    testBoundedTableMap.put("table_with_map", TestInput.TABLE_WITH_MAP);
    testBoundedTableMap.put("table_with_date", TestInput.TABLE_WITH_DATE);
    testBoundedTableMap.put("table_with_time", TestInput.TABLE_WITH_TIME);
    testBoundedTableMap.put("table_with_numeric", TestInput.TABLE_WITH_NUMERIC);
    testBoundedTableMap.put("table_with_datetime", TestInput.TABLE_WITH_DATETIME);
    testBoundedTableMap.put(
        "table_with_struct_ts_string", TestInput.TABLE_WITH_STRUCT_TIMESTAMP_STRING);
    testBoundedTableMap.put("streaming_sql_test_table_a", TestInput.STREAMING_SQL_TABLE_A);
    testBoundedTableMap.put("streaming_sql_test_table_b", TestInput.STREAMING_SQL_TABLE_B);

    return new ReadOnlyTableProvider("test_table_provider", testBoundedTableMap);
  }

  protected void initialize() {
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(createBeamTableProvider(), PipelineOptionsFactory.create());

    this.config =
        Frameworks.newConfigBuilder()
            .defaultSchema(jdbcConnection.getCurrentSchemaPlus())
            .traitDefs(ImmutableList.of(ConventionTraitDef.INSTANCE))
            .context(Contexts.of(jdbcConnection.config()))
            .ruleSets(ZetaSQLQueryPlanner.getZetaSqlRuleSets().toArray(new RuleSet[0]))
            .costFactory(BeamCostModel.FACTORY)
            .typeSystem(jdbcConnection.getTypeFactory().getTypeSystem())
            .build();
  }
}
