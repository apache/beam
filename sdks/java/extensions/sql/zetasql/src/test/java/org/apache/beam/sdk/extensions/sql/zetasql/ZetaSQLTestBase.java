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

import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.AGGREGATE_TABLE_ONE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.AGGREGATE_TABLE_TWO;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.BASIC_TABLE_ONE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.BASIC_TABLE_THREE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.BASIC_TABLE_TWO;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.STREAMING_SQL_TABLE_A;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.STREAMING_SQL_TABLE_B;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_ALL_NULL;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_ALL_TYPES;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_ALL_TYPES_2;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_EMPTY;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_FOR_CASE_WHEN;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_ARRAY;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_ARRAY_FOR_UNNEST;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_DATE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_MAP;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_STRUCT;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_STRUCT_TIMESTAMP_STRING;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_STRUCT_TWO;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TABLE_WITH_TIME;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TIMESTAMP_TABLE_ONE;
import static org.apache.beam.sdk.extensions.sql.zetasql.TestInput.TIMESTAMP_TABLE_TWO;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Context;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Contexts;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitDef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** ZetaSQLTestBase. */
public class ZetaSQLTestBase {
  protected static final Long PIPELINE_EXECUTION_WAITTIME_MINUTES = 2L;

  protected FrameworkConfig config;

  protected TableProvider tableProvider;

  protected void initializeBeamTableProvider() {
    Map<String, BeamSqlTable> testBoundedTableMap = new HashMap<>();
    testBoundedTableMap.put("KeyValue", BASIC_TABLE_ONE);
    testBoundedTableMap.put("BigTable", BASIC_TABLE_TWO);
    testBoundedTableMap.put("Spanner", BASIC_TABLE_THREE);
    testBoundedTableMap.put("aggregate_test_table", AGGREGATE_TABLE_ONE);
    testBoundedTableMap.put("window_test_table", TIMESTAMP_TABLE_ONE);
    testBoundedTableMap.put("window_test_table_two", TIMESTAMP_TABLE_TWO);
    testBoundedTableMap.put("all_null_table", TABLE_ALL_NULL);
    testBoundedTableMap.put("table_with_struct", TABLE_WITH_STRUCT);
    testBoundedTableMap.put("table_with_struct_two", TABLE_WITH_STRUCT_TWO);
    testBoundedTableMap.put("table_with_array", TABLE_WITH_ARRAY);
    testBoundedTableMap.put("table_with_array_for_unnest", TABLE_WITH_ARRAY_FOR_UNNEST);
    testBoundedTableMap.put("table_for_case_when", TABLE_FOR_CASE_WHEN);
    testBoundedTableMap.put("aggregate_test_table_two", AGGREGATE_TABLE_TWO);
    testBoundedTableMap.put("table_empty", TABLE_EMPTY);
    testBoundedTableMap.put("table_all_types", TABLE_ALL_TYPES);
    testBoundedTableMap.put("table_all_types_2", TABLE_ALL_TYPES_2);
    testBoundedTableMap.put("table_with_map", TABLE_WITH_MAP);
    testBoundedTableMap.put("table_with_date", TABLE_WITH_DATE);
    testBoundedTableMap.put("table_with_time", TABLE_WITH_TIME);
    testBoundedTableMap.put("table_with_struct_ts_string", TABLE_WITH_STRUCT_TIMESTAMP_STRING);
    testBoundedTableMap.put("streaming_sql_test_table_a", STREAMING_SQL_TABLE_A);
    testBoundedTableMap.put("streaming_sql_test_table_b", STREAMING_SQL_TABLE_B);

    tableProvider = new ReadOnlyTableProvider("test_table_provider", testBoundedTableMap);
  }

  protected void initializeCalciteEnvironment() {
    initializeCalciteEnvironmentWithContext();
  }

  protected void initializeCalciteEnvironmentWithContext(Context... extraContext) {
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());
    SchemaPlus defaultSchemaPlus = jdbcConnection.getCurrentSchemaPlus();
    final ImmutableList<RelTraitDef> traitDefs = ImmutableList.of(ConventionTraitDef.INSTANCE);

    Object[] contexts =
        ImmutableList.<Context>builder()
            .add(Contexts.of(jdbcConnection.config()))
            .add(extraContext)
            .build()
            .toArray();

    this.config =
        Frameworks.newConfigBuilder()
            .defaultSchema(defaultSchemaPlus)
            .traitDefs(traitDefs)
            .context(Contexts.of(contexts))
            .ruleSets(ZetaSQLQueryPlanner.getZetaSqlRuleSets())
            .costFactory(BeamCostModel.FACTORY)
            .typeSystem(jdbcConnection.getTypeFactory().getTypeSystem())
            .build();
  }
}
