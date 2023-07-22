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

/**
 * This class generates a unique name for the partition metadata table, which is created when the
 * Connector is initialized.
 */
public class NameGenerator {

  private static final String PARTITION_METADATA_TABLE_NAME_FORMAT = "Metadata_%s_%s";
  private static final int MAX_TABLE_NAME_LENGTH = 63;

  /**
   * Generates an unique name for the partition metadata table in the form of {@code
   * "Metadata_<databaseId>_<uuid>"}.
   *
   * @param databaseId The database id where the table will be created
   * @return the unique generated name of the partition metadata table
   */
  public static String generatePartitionMetadataTableName(String databaseId) {
    // There are 11 characters in the name format.
    // Maximum Spanner database ID length is 30 characters.
    // UUID always generates a String with 36 characters.
    // Since the Postgres table name length is 63, we may need to truncate the table name depending
    // on the database length.
    String fullString =
        String.format(PARTITION_METADATA_TABLE_NAME_FORMAT, databaseId, UUID.randomUUID())
            .replaceAll("-", "_");
    if (fullString.length() < MAX_TABLE_NAME_LENGTH) {
      return fullString;
    }
    return fullString.substring(0, MAX_TABLE_NAME_LENGTH);
  }
}
