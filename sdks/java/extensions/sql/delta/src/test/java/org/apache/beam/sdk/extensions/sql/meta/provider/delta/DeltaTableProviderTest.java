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
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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

  @Test
  public void testForReadRejectsOptions() {
    Table table = fakeTable("my_table", "/path/to/delta/table", "{}");
    DeltaTable deltaTable = (DeltaTable) provider.buildBeamSqlTable(table);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                deltaTable.forRead(
                    ImmutableMap.of("include_metadata_columns", ImmutableList.of("_change_type"))));
    assertEquals(
        "WITH options on a Delta Lake read are only accepted with CHANGES(...), got"
            + " [include_metadata_columns]",
        exception.getMessage());
  }

  @Test
  public void testForChangesBuildsDeltaChangesTable() {
    Table table =
        fakeTable(
            "my_table",
            "/path/to/delta/table",
            "{\"hadoop_config\": {\"fs.gs.project.id\": \"my-project\"}}");
    DeltaTable deltaTable = (DeltaTable) provider.buildBeamSqlTable(table);

    DeltaChangesTable changesTable =
        (DeltaChangesTable)
            deltaTable.forChanges(
                ChangeRange.versions(1L, 3L),
                ImmutableMap.of(
                    "include_metadata_columns",
                    ImmutableList.of("_commit_timestamp", "_change_type", "_commit_version")),
                ImmutableList.of());

    assertEquals(
        ImmutableList.of("id", "name", "_change_type", "_commit_version", "_commit_timestamp"),
        changesTable.getSchema().getFieldNames());
    assertEquals(
        Schema.FieldType.STRING, changesTable.getSchema().getField("_change_type").getType());
    assertEquals(
        Schema.FieldType.INT64, changesTable.getSchema().getField("_commit_version").getType());
    assertEquals(
        Schema.FieldType.DATETIME,
        changesTable.getSchema().getField("_commit_timestamp").getType());
    assertEquals(
        ImmutableMap.of(
            "table",
            "/path/to/delta/table",
            "hadoop_config",
            ImmutableMap.of("fs.gs.project.id", "my-project"),
            "include_metadata_columns",
            ImmutableList.of("_change_type", "_commit_version", "_commit_timestamp")),
        changesTable.getConfig());
    assertEquals(PCollection.IsBounded.BOUNDED, changesTable.isBounded());
    UnsupportedOperationException writeError =
        assertThrows(UnsupportedOperationException.class, () -> changesTable.buildIOWriter(null));
    assertEquals(
        "CHANGES of Delta Lake table '/path/to/delta/table' cannot be written.",
        writeError.getMessage());
  }

  @Test
  public void testForChangesValidationErrors() {
    DeltaTable deltaTable =
        (DeltaTable)
            provider.buildBeamSqlTable(fakeTable("my_table", "/path/to/delta/table", "{}"));
    DeltaTable pinnedVersionTable =
        (DeltaTable)
            provider.buildBeamSqlTable(
                fakeTable("my_table", "/path/to/delta/table", "{\"version\": 1}"));
    DeltaTable pinnedTimestampTable =
        (DeltaTable)
            provider.buildBeamSqlTable(
                fakeTable(
                    "my_table",
                    "/path/to/delta/table",
                    "{\"timestamp\": \"2026-01-01T00:00:00Z\"}"));

    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    pinnedVersionTable.forChanges(
                        ChangeRange.versions(0L, 1L), ImmutableMap.of(), ImmutableList.of()))
            .getMessage()
            .contains("pins the version it reads"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    pinnedTimestampTable.forChanges(
                        ChangeRange.versions(0L, 1L), ImmutableMap.of(), ImmutableList.of()))
            .getMessage()
            .contains("pins the version it reads"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(ChangeRange.all(), ImmutableMap.of(), ImmutableList.of()))
            .getMessage()
            .contains("needs a start"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(
                        ChangeRange.versions(null, 2L), ImmutableMap.of(), ImmutableList.of()))
            .getMessage()
            .contains("needs a start"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(
                        ChangeRange.versions(0L, 1L),
                        ImmutableMap.of("start_version", 0L),
                        ImmutableList.of()))
            .getMessage()
            .contains("Option 'start_version' cannot be set in WITH"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(
                        ChangeRange.versions(0L, 1L),
                        ImmutableMap.of("table", "/other"),
                        ImmutableList.of()))
            .getMessage()
            .contains("Option 'table' cannot be set in WITH"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(
                        ChangeRange.versions(0L, 1L),
                        ImmutableMap.of("bogus", 1L),
                        ImmutableList.of()))
            .getMessage()
            .contains("Unknown option 'bogus' for a CHANGES read"));
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    deltaTable.forChanges(
                        ChangeRange.versions(0L, 1L),
                        ImmutableMap.of("include_metadata_columns", ImmutableList.of("_bogus")),
                        ImmutableList.of()))
            .getMessage()
            .contains("Unknown metadata column '_bogus'"));

    DeltaTable collidingTable =
        (DeltaTable)
            provider.buildBeamSqlTable(
                Table.builder()
                    .name("colliding")
                    .location("/path/to/delta/table")
                    .schema(Schema.builder().addStringField("_change_type").build())
                    .type("delta")
                    .properties(TableUtils.parseProperties("{}"))
                    .build());
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    collidingTable.forChanges(
                        ChangeRange.versions(0L, 1L),
                        ImmutableMap.of(
                            "include_metadata_columns", ImmutableList.of("_change_type")),
                        ImmutableList.of()))
            .getMessage()
            .contains("Metadata column '_change_type' collides with a column of the table"));
  }
}
