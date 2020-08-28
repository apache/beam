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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Contexts;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Common setup for ZetaSQL tests. */
public abstract class ZetaSqlTestBase {
  protected static final long PIPELINE_EXECUTION_WAITTIME_MINUTES = 2L;

  // The field has been initialized in initialize method
  @SuppressWarnings("initialization.fields.uninitialized")
  protected FrameworkConfig config;

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
