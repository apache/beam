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
package org.apache.beam.sdk.extensions.sql.meta.provider.delta;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeltaTableProvider} and {@link DeltaTable}. */
@RunWith(JUnit4.class)
public class DeltaTableProviderTest {

  private final DeltaTableProvider provider = new DeltaTableProvider();

  private static Table fakeTable(String name, String location, String properties) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(location)
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("delta")
        .properties(TableUtils.parseProperties(properties))
        .build();
  }

  @Test
  public void testGetTableType() {
    assertEquals("delta", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTableBasic() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof DeltaTable);

    DeltaTable deltaTable = (DeltaTable) sqlTable;
    assertEquals("/path/to/delta/table", deltaTable.tableLocation);
    assertNull(deltaTable.version);
    assertNull(deltaTable.timestamp);
    assertNull(deltaTable.hadoopConfig);
    assertEquals(PCollection.IsBounded.BOUNDED, deltaTable.isBounded());
    assertEquals(ProjectSupport.NONE, deltaTable.supportsProjects());
  }

  @Test
  public void testBuildBeamSqlTableWithVersion() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{\"version\": 5}");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertTrue(sqlTable instanceof DeltaTable);
    DeltaTable deltaTable = (DeltaTable) sqlTable;
    assertEquals(Long.valueOf(5L), deltaTable.version);
    assertNull(deltaTable.timestamp);
  }

  @Test
  public void testBuildBeamSqlTableWithTimestamp() {
    Table table =
        fakeTable("my_table", "/path/to/delta/table", "{\"timestamp\": \"2026-05-20T15:43:26Z\"}");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertTrue(sqlTable instanceof DeltaTable);
    DeltaTable deltaTable = (DeltaTable) sqlTable;
    assertEquals("2026-05-20T15:43:26Z", deltaTable.timestamp);
    assertNull(deltaTable.version);
  }

  @Test
  public void testBuildBeamSqlTableWithHadoopConfig() {
    Table table =
        fakeTable(
            "my_table",
            "/path/to/delta/table",
            "{\"hadoop_config\": {\"fs.gs.project.id\": \"my-project\", \"foo\": \"bar\"}}");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertTrue(sqlTable instanceof DeltaTable);
    DeltaTable deltaTable = (DeltaTable) sqlTable;
    assertNotNull(deltaTable.hadoopConfig);
    assertEquals("my-project", deltaTable.hadoopConfig.get("fs.gs.project.id"));
    assertEquals("bar", deltaTable.hadoopConfig.get("foo"));
  }

  @Test
  public void testBuildBeamSqlTableWithoutLocationFails() {
    Table table = fakeTable("my_table", null, "{}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Delta Lake table location must be specified (catalog-based tables are not supported)."));
  }

  @Test
  public void testCreateTableWithoutLocationFails() {
    Table table = fakeTable("my_table", null, "{}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.createTable(table));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Delta Lake table location must be specified (catalog-based tables are not supported)."));
  }

  @Test
  public void testBuildBeamSqlTableWithBothVersionAndTimestampFails() {
    Table table =
        fakeTable(
            "my_table",
            "/path/to/delta/table",
            "{\"version\": 1, \"timestamp\": \"2026-05-20T15:43:26Z\"}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    assertTrue(exception.getMessage().contains("Cannot set both version and timestamp."));
  }

  @Test
  public void testBuildBeamSqlTableWithWritePropertyFails() {
    Table table =
        fakeTable(
            "my_table",
            "/path/to/delta/table",
            "{\"beam.write.triggering_frequency_seconds\": 30}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    assertTrue(
        exception
            .getMessage()
            .contains("Writing to Delta Lake tables is currently not supported."));
  }

  @Test
  public void testDirectDeltaTableConstructor() {
    Table table = fakeTable("my_table", null, "{\"version\": 2}");
    DeltaTable deltaTable = new DeltaTable("/direct/path", table);

    assertEquals("/direct/path", deltaTable.tableLocation);
    assertEquals(Long.valueOf(2L), deltaTable.version);
    assertNull(deltaTable.timestamp);
    assertNull(deltaTable.hadoopConfig);
  }

  @Test
  public void testBuildBeamSqlTableWithCamelCaseHadoopConfig() {
    Table table =
        fakeTable(
            "my_table",
            "/path/to/delta/table",
            "{\"hadoopConfig\": {\"fs.gs.project.id\": \"my-project\"}}");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertTrue(sqlTable instanceof DeltaTable);
    DeltaTable deltaTable = (DeltaTable) sqlTable;
    assertNotNull(deltaTable.hadoopConfig);
    assertEquals("my-project", deltaTable.hadoopConfig.get("fs.gs.project.id"));
  }

  @Test
  public void testBuildBeamSqlTableWithUnknownBeamReadPropertyFails() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{\"beam.read.unsupported\": 123}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    assertTrue(
        exception.getMessage().contains("Unknown Beam read property: beam.read.unsupported"));
  }

  @Test
  public void testBuildBeamSqlTableWithUnknownPropertyFails() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{\"unsupported_property\": 123}");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    assertTrue(exception.getMessage().contains("Unknown property 'unsupported_property'"));
  }

  @Test
  public void testSupportsPartitioning() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    assertFalse(provider.supportsPartitioning(table));
  }

  @Test
  public void testAlterTableFails() {
    assertThrows(UnsupportedOperationException.class, () -> provider.alterTable("my_table"));
  }

  @Test
  public void testBuildIOWriterFails() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    DeltaTable deltaTable = (DeltaTable) provider.buildBeamSqlTable(table);
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> deltaTable.buildIOWriter(null));
    assertTrue(
        exception
            .getMessage()
            .contains("Writing to Delta Lake tables is currently not supported."));
  }

  @Test
  public void testConstructFilterReturnsDefaultTableFilter() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    DeltaTable deltaTable = (DeltaTable) provider.buildBeamSqlTable(table);
    BeamSqlTableFilter filter = deltaTable.constructFilter(Collections.emptyList());
    assertTrue(filter instanceof DefaultTableFilter);
    assertEquals(0, filter.numSupported());
  }

  @Test
  public void testBuildIOReaderWithPushDownFails() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    DeltaTable deltaTable = (DeltaTable) provider.buildBeamSqlTable(table);

    BeamSqlTableFilter nonDefaultFilter =
        new BeamSqlTableFilter() {
          @Override
          public java.util.List<RexNode> getNotSupported() {
            return Collections.emptyList();
          }

          @Override
          public int numSupported() {
            return 1;
          }
        };

    assertThrows(
        UnsupportedOperationException.class,
        () -> deltaTable.buildIOReader(null, nonDefaultFilter, Collections.emptyList()));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            deltaTable.buildIOReader(
                null, new DefaultTableFilter(Collections.emptyList()), ImmutableList.of("id")));
  }
}
