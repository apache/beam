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
import static org.junit.Assert.*;

import org.junit.Test;

public class PartitionMetadataTableNamesTest {

  @Test
  public void testGenerateMetadataConfigRemovesHyphens() {
    PartitionMetadataTableNames names = PartitionMetadataTableNames.from("my-database-id-12345");
    assertFalse(names.getTableName().contains("-"));
    assertFalse(names.getWatermarkIndexName().contains("-"));
    assertFalse(names.getCreatedAtIndexName().contains("-"));
  }

  @Test
  public void testGenerateMetadataConfigIsShorterThan64Characters() {
    PartitionMetadataTableNames names =
        PartitionMetadataTableNames.from("my-database-id1-maximum-length");
    assertTrue(names.getTableName().length() <= MAX_NAME_LENGTH);
    assertTrue(names.getWatermarkIndexName().length() <= MAX_NAME_LENGTH);
    assertTrue(names.getCreatedAtIndexName().length() <= MAX_NAME_LENGTH);
  }
}
