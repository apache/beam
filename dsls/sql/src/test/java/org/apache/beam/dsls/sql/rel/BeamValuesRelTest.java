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

package org.apache.beam.dsls.sql.rel;

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamValuesRel}.
 */
public class BeamValuesRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static MockedBeamSqlTable stringTable = MockedBeamSqlTable
      .of(SqlTypeName.VARCHAR, "name",
          SqlTypeName.VARCHAR, "description");

  private static MockedBeamSqlTable intTable = MockedBeamSqlTable
      .of(SqlTypeName.INTEGER, "c0",
          SqlTypeName.INTEGER, "c1");

  @Test
  public void testValues() throws Exception {
    String sql = "insert into string_table(name, description) values "
        + "('hello', 'world'), ('james', 'bond')";
    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.VARCHAR, "name",
        SqlTypeName.VARCHAR, "description",
        "hello", "world",
        "james", "bond").getInputRecords());
    pipeline.run();
  }

  @Test
  public void testValues_castInt() throws Exception {
    String sql = "insert into int_table (c0, c1) values(cast(1 as int), cast(2 as int))";
    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "c0",
        SqlTypeName.INTEGER, "c1",
        1, 2
    ).getInputRecords());
    pipeline.run();
  }

  @Test
  public void testValues_onlySelect() throws Exception {
    String sql = "select 1, '1'";
    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "EXPR$0",
        SqlTypeName.CHAR, "EXPR$1",
        1, "1"
    ).getInputRecords());
    pipeline.run();
  }

  @BeforeClass
  public static void prepareClass() {
    sqlEnv.registerTable("string_table", stringTable);
    sqlEnv.registerTable("int_table", intTable);
  }

  @Before
  public void prepare() {
    MockedBeamSqlTable.CONTENT.clear();
  }
}
