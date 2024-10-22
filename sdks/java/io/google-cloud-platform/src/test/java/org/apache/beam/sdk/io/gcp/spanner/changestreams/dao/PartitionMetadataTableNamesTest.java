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
    assertTrue(names.getWatermarkIndexName().length() <= MAX_NAME_LENGTH);
    assertTrue(names.getCreatedAtIndexName().length() <= MAX_NAME_LENGTH);
  }

  @Test
  public void testPartitionMetadataNamesFromExistingTable() {
    PartitionMetadataTableNames names1 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", "mytable");
    assertEquals("mytable", names1.getTableName());
    assertFalse(names1.getWatermarkIndexName().contains("-"));
    assertFalse(names1.getCreatedAtIndexName().contains("-"));

    PartitionMetadataTableNames names2 =
        PartitionMetadataTableNames.fromExistingTable("databaseid", "mytable");
    assertEquals("mytable", names2.getTableName());
    assertNotEquals(names1.getWatermarkIndexName(), names2.getWatermarkIndexName());
    assertNotEquals(names1.getCreatedAtIndexName(), names2.getCreatedAtIndexName());
  }
}
