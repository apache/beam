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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link TableResolution}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TableResolutionTest {

  // A simple in-memory SchemaPlus would be fine
  @Mock SchemaPlus mockSchemaPlus;
  @Mock SchemaPlus innerSchemaPlus;

  // A table whose identity is not important
  @Mock Table mockTable;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  /** Unit test for resolving a table with no hierarchy. */
  @Test
  public void testResolveFlat() {
    String tableName = "fake_table";
    when(mockSchemaPlus.getTable(tableName)).thenReturn(mockTable);
    Table table = TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(tableName));
    assertThat(table, Matchers.is(mockTable));
  }

  /** Unit test for resolving a table with no hierarchy but dots in its actual name. */
  @Test
  public void testResolveWithDots() {
    String tableName = "fake.table";
    when(mockSchemaPlus.getTable(tableName)).thenReturn(mockTable);
    Table table = TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(tableName));
    assertThat(table, Matchers.is(mockTable));
  }

  /** Unit test for failing to resolve a table with no subschemas. */
  @Test
  public void testMissingFlat() {
    String tableName = "fake_table";
    when(mockSchemaPlus.getTable(tableName)).thenReturn(null);
    Table table = TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(tableName));
    assertThat(table, Matchers.nullValue());
  }

  /** Unit test for resolving a table with some hierarchy. */
  @Test
  public void testResolveNested() {
    String subSchema = "fake_schema";
    String tableName = "fake_table";
    when(mockSchemaPlus.getSubSchema(subSchema)).thenReturn(innerSchemaPlus);
    when(innerSchemaPlus.getTable(tableName)).thenReturn(mockTable);
    Table table =
        TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(subSchema, tableName));
    assertThat(table, Matchers.is(mockTable));
  }

  /** Unit test for resolving a table with dots in the subschema names and the table name. */
  @Test
  public void testResolveNestedWithDots() {
    String subSchema = "fake.schema";
    String tableName = "fake.table";
    when(mockSchemaPlus.getSubSchema(subSchema)).thenReturn(innerSchemaPlus);
    when(innerSchemaPlus.getTable(tableName)).thenReturn(mockTable);
    Table table =
        TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(subSchema, tableName));
    assertThat(table, Matchers.is(mockTable));
  }

  /** Unit test for resolving a table with some hierarchy that is missing. */
  @Test
  public void testMissingSubschema() {
    String subSchema = "fake_schema";
    String tableName = "fake_table";
    when(mockSchemaPlus.getSubSchema(subSchema)).thenReturn(null);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> {
          TableResolution.resolveCalciteTable(
              mockSchemaPlus, ImmutableList.of(subSchema, tableName));
        });
  }

  /** Unit test for resolving a table with some hierarchy and the table is missing. */
  @Test
  public void testMissingTableInSubschema() {
    String subSchema = "fake_schema";
    String tableName = "fake_table";
    when(mockSchemaPlus.getSubSchema(subSchema)).thenReturn(innerSchemaPlus);
    when(innerSchemaPlus.getTable(tableName)).thenReturn(null);
    Table table =
        TableResolution.resolveCalciteTable(mockSchemaPlus, ImmutableList.of(subSchema, tableName));
    assertThat(table, Matchers.nullValue());
  }
}
