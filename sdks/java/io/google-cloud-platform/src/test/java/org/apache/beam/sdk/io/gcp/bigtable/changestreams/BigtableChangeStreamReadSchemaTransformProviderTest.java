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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.data.v2.models.AddToCell;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.MergeToCell;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.bigtable.data.v2.models.Value;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.BigtableChangeStreamReadSchemaTransformProvider.BigtableChangeStreamReadConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

public class BigtableChangeStreamReadSchemaTransformProviderTest {

  @Test
  public void testIntValueToRow() {
    Row row = BigtableChangeStreamReadSchemaTransformProvider.valueToRow(Value.intValue(123L));

    assertEquals("INT64", row.getString("type"));
    assertEquals(Long.valueOf(123L), row.getInt64("int_value"));
    assertNull(row.getInt64("raw_timestamp_micros"));
    assertNull(row.getBytes("raw_value"));
  }

  @Test
  public void testRawTimestampValueToRow() {
    Row row =
        BigtableChangeStreamReadSchemaTransformProvider.valueToRow(Value.rawTimestamp(123456L));

    assertEquals("RAW_TIMESTAMP", row.getString("type"));
    assertNull(row.getInt64("int_value"));
    assertEquals(Long.valueOf(123456L), row.getInt64("raw_timestamp_micros"));
    assertNull(row.getBytes("raw_value"));
  }

  @Test
  public void testRawValueToRow() {
    byte[] value = "value".getBytes(UTF_8);

    Row row =
        BigtableChangeStreamReadSchemaTransformProvider.valueToRow(
            Value.rawValue(ByteString.copyFrom(value)));

    assertEquals("RAW_VALUE", row.getString("type"));
    assertNull(row.getInt64("int_value"));
    assertNull(row.getInt64("raw_timestamp_micros"));
    assertArrayEquals(value, row.getBytes("raw_value"));
  }

  @Test
  public void testTimestampRangeToRow() {
    Range.TimestampRange range = Range.TimestampRange.create(100L, 200L);

    Row row = BigtableChangeStreamReadSchemaTransformProvider.timestampRangeToRow(range);

    assertEquals("CLOSED", row.getString("start_bound"));
    assertEquals(Long.valueOf(100L), row.getInt64("start_timestamp_micros"));
    assertEquals("OPEN", row.getString("end_bound"));
    assertEquals(Long.valueOf(200L), row.getInt64("end_timestamp_micros"));
  }

  @Test
  public void testUnboundedTimestampRangeToRow() {
    Range.TimestampRange range = Range.TimestampRange.unbounded();

    Row row = BigtableChangeStreamReadSchemaTransformProvider.timestampRangeToRow(range);

    assertEquals("UNBOUNDED", row.getString("start_bound"));
    assertNull(row.getInt64("start_timestamp_micros"));
    assertEquals("UNBOUNDED", row.getString("end_bound"));
    assertNull(row.getInt64("end_timestamp_micros"));
  }

  @Test
  public void testSetCellEntryToRow() {
    SetCell setCell =
        SetCell.create(
            "family", ByteString.copyFromUtf8("qualifier"), 123L, ByteString.copyFromUtf8("value"));

    Row row = BigtableChangeStreamReadSchemaTransformProvider.entryToRow(setCell);

    assertEquals("SET_CELL", row.getString("type"));
    assertEquals("family", row.getString("family_name"));
    assertArrayEquals("qualifier".getBytes(UTF_8), row.getBytes("qualifier"));
    assertEquals(Long.valueOf(123L), row.getInt64("timestamp_micros"));
    assertArrayEquals("value".getBytes(UTF_8), row.getBytes("value"));

    assertNull(row.getRow("timestamp_range"));
    assertNull(row.getRow("value_qualifier"));
    assertNull(row.getRow("value_timestamp"));
    assertNull(row.getRow("value_input"));
  }

  @Test
  public void testDeleteFamilyEntryToRow() {
    DeleteFamily deleteFamily = DeleteFamily.create("family");

    Row row = BigtableChangeStreamReadSchemaTransformProvider.entryToRow(deleteFamily);

    assertEquals("DELETE_FAMILY", row.getString("type"));
    assertEquals("family", row.getString("family_name"));

    assertNull(row.getBytes("qualifier"));
    assertNull(row.getInt64("timestamp_micros"));
    assertNull(row.getBytes("value"));
    assertNull(row.getRow("timestamp_range"));
    assertNull(row.getRow("value_qualifier"));
    assertNull(row.getRow("value_timestamp"));
    assertNull(row.getRow("value_input"));
  }

  @Test
  public void testConfigurationValidation() {
    BigtableChangeStreamReadConfiguration configuration =
        BigtableChangeStreamReadConfiguration.builder()
            .setProjectId("project")
            .setInstanceId("instance")
            .setTableId("table")
            .build();

    configuration.validate();
  }

  @Test
  public void testConfigurationRejectsEmptyProject() {
    BigtableChangeStreamReadConfiguration configuration =
        BigtableChangeStreamReadConfiguration.builder()
            .setProjectId("")
            .setInstanceId("instance")
            .setTableId("table")
            .build();

    assertThrows(IllegalArgumentException.class, configuration::validate);
  }

  @Test
  public void testConfigurationRejectsEmptyInstance() {
    BigtableChangeStreamReadConfiguration configuration =
        BigtableChangeStreamReadConfiguration.builder()
            .setProjectId("project")
            .setInstanceId("")
            .setTableId("table")
            .build();

    assertThrows(IllegalArgumentException.class, configuration::validate);
  }

  @Test
  public void testConfigurationRejectsEmptyTable() {
    BigtableChangeStreamReadConfiguration configuration =
        BigtableChangeStreamReadConfiguration.builder()
            .setProjectId("project")
            .setInstanceId("instance")
            .setTableId("")
            .build();

    assertThrows(IllegalArgumentException.class, configuration::validate);
  }

  @Test
  public void testDeleteCellsEntryToRow() {
    DeleteCells deleteCells =
        DeleteCells.create(
            "family",
            ByteString.copyFromUtf8("qualifier"),
            Range.TimestampRange.create(100L, 200L));

    Row row = BigtableChangeStreamReadSchemaTransformProvider.entryToRow(deleteCells);

    assertEquals("DELETE_CELLS", row.getString("type"));
    assertEquals("family", row.getString("family_name"));
    assertArrayEquals("qualifier".getBytes(UTF_8), row.getBytes("qualifier"));

    Row range = row.getRow("timestamp_range");
    assertEquals("CLOSED", range.getString("start_bound"));
    assertEquals(Long.valueOf(100L), range.getInt64("start_timestamp_micros"));
    assertEquals("OPEN", range.getString("end_bound"));
    assertEquals(Long.valueOf(200L), range.getInt64("end_timestamp_micros"));
  }

  @Test
  public void testAddToCellEntryToRow() {
    AddToCell addToCell =
        AddToCell.create(
            "family",
            Value.rawValue(ByteString.copyFromUtf8("qualifier")),
            Value.rawTimestamp(123L),
            Value.intValue(42L));

    Row row = BigtableChangeStreamReadSchemaTransformProvider.entryToRow(addToCell);

    assertEquals("ADD_TO_CELL", row.getString("type"));
    assertEquals("family", row.getString("family_name"));

    Row qualifier = row.getRow("value_qualifier");
    assertEquals("RAW_VALUE", qualifier.getString("type"));
    assertArrayEquals("qualifier".getBytes(UTF_8), qualifier.getBytes("raw_value"));

    Row timestamp = row.getRow("value_timestamp");
    assertEquals("RAW_TIMESTAMP", timestamp.getString("type"));
    assertEquals(Long.valueOf(123L), timestamp.getInt64("raw_timestamp_micros"));

    Row input = row.getRow("value_input");
    assertEquals("INT64", input.getString("type"));
    assertEquals(Long.valueOf(42L), input.getInt64("int_value"));
  }

  @Test
  public void testMergeToCellEntryToRow() {
    MergeToCell mergeToCell =
        MergeToCell.create(
            "family",
            Value.rawValue(ByteString.copyFromUtf8("qualifier")),
            Value.rawTimestamp(123L),
            Value.rawValue(ByteString.copyFromUtf8("input")));

    Row row = BigtableChangeStreamReadSchemaTransformProvider.entryToRow(mergeToCell);

    assertEquals("MERGE_TO_CELL", row.getString("type"));
    assertEquals("family", row.getString("family_name"));

    Row input = row.getRow("value_input");
    assertEquals("RAW_VALUE", input.getString("type"));
    assertArrayEquals("input".getBytes(UTF_8), input.getBytes("raw_value"));
  }

  @Test
  public void testConfigurationSchema() {
    BigtableChangeStreamReadSchemaTransformProvider provider =
        new BigtableChangeStreamReadSchemaTransformProvider();

    Schema schema = provider.configurationSchema();

    assertEquals(
        Set.of(
            "project_id",
            "instance_id",
            "table_id",
            "app_profile_id",
            "start_at_timestamp",
            "change_stream_name"),
        new HashSet<>(schema.getFieldNames()));
  }
}
