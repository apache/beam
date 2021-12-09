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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import java.util.UUID;

// TODO: Add java docs
public class NameGenerator {

  private static final String PARTITION_METADATA_TABLE_NAME_FORMAT =
      "CDC_Partitions_Metadata_%s_%s";

  // TODO: Add java docs
  public static String generatePartitionMetadataTableName(String databaseId) {
    // Maximum Spanner table name length is 128 characters.
    // There are 25 characters in the name format.
    // Maximum Spanner database ID length is 30 characters.
    // UUID always generates a String with 36 characters.
    // 128 - (25 + 30 + 36) = 37 characters short of the limit
    return String.format(PARTITION_METADATA_TABLE_NAME_FORMAT, databaseId, UUID.randomUUID())
        .replaceAll("-", "_");
  }
}
