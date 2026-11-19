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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataTableNames.MAX_NAME_LENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PartitionMetadataTableNamesTest {
  @Test
  public void testGeneratePartitionMetadataNamesRemovesHyphens() {
    String databaseId = "my-database-id-12345";

    PartitionMetadataTableNames names1 = PartitionMetadataTableNames.generateRandom(databaseId);
    assertFalse(names1.getTableName().contains("-"));
    assertFalse(names1.getWatermarkIndexName().contains("-"));
    assertFalse(names1.getCreatedAtIndexName().contains("-"));
    assertTrue(names1.getWatermarkIndexName().startsWith("WatermarkIdx_Metadata_"));
    assertTrue(names1.getCreatedAtIndexName().startsWith("CreatedAtIdx_Metadata_"));
    assertEquals(
        names1, PartitionMetadataTableNames.fromExistingTable(databaseId, names1.getTableName()));

    PartitionMetadataTableNames names2 = PartitionMetadataTableNames.generateRandom(databaseId);
    assertNotEquals(names1.getTableName(), names2.getTableName());
    assertNotEquals(names1.getWatermarkIndexName(), names2.getWatermarkIndexName());
    assertNotEquals(names1.getCreatedAtIndexName(), names2.getCreatedAtIndexName());
  }

  @Test
  public void testGeneratePartitionMetadataNamesIsShorterThan64Characters() {
    PartitionMetadataTableNames names =
        PartitionMetadataTableNames.generateRandom(
            "my-database-id-larger-than-maximum-length-1234567890-1234567890-1234567890");
    assertTrue(names.getTableName().length() <= MAX_NAME_LENGTH);
    assertTrue(names.getWatermarkIndexName().length() <= MAX_NAME_LENGTH);
    assertTrue(names.getCreatedAtIndexName().length() <= MAX_NAME_LENGTH);

    names = PartitionMetadataTableNames.generateRandom("d");
    assertTrue(names.getTableName().length() <= MAX_NAME_LENGTH);
    assertEquals("WatermarkIdx_" + names.getTableName(), names.getWatermarkIndexName());
    assertEquals("CreatedAtIdx_" + names.getTableName(), names.getCreatedAtIndexName());
  }

  @Test
  public void testPartitionMetadataNamesFromExistingTable() {
    PartitionMetadataTableNames names1 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", "mytable");
    assertEquals("mytable", names1.getTableName());
    assertEquals("WatermarkIdx_mytable", names1.getWatermarkIndexName());
    assertEquals("CreatedAtIdx_mytable", names1.getCreatedAtIndexName());

    PartitionMetadataTableNames names2 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", "mytable");
    assertEquals(names1, names2);

    PartitionMetadataTableNames hyphenNames =
        PartitionMetadataTableNames.fromExistingTable("databaseid", "my-table");
    assertEquals("my-table", hyphenNames.getTableName());
    assertEquals("WatermarkIdx_my_table", hyphenNames.getWatermarkIndexName());
    assertEquals("CreatedAtIdx_my_table", hyphenNames.getCreatedAtIndexName());
  }

  @Test
  public void testPartitionMetadataNamesFromExistingLongTableIsShorterThan64Characters() {
    String longTable = "my_metadata_table_larger_than_maximum_length_1234567890_1234567890";

    PartitionMetadataTableNames names1 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", longTable);
    PartitionMetadataTableNames names2 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", longTable);

    assertEquals(longTable, names1.getTableName());
    assertEquals(MAX_NAME_LENGTH, names1.getWatermarkIndexName().length());
    assertEquals(MAX_NAME_LENGTH, names1.getCreatedAtIndexName().length());
    assertFalse(names1.getWatermarkIndexName().contains("-"));
    assertFalse(names1.getCreatedAtIndexName().contains("-"));
    assertEquals(names1, names2);
  }
}
