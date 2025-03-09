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
package org.apache.beam.it.clickhouse;

import java.util.regex.Pattern;

/** Utilities for {@link ClickHouseResourceManager} implementations. */
public class ClickHouseUtils {

  // ClickHouse max table name length
  private static final int MAX_TABLE_NAME_LENGTH = 206;

  // ClickHouse table name restrictions:
  // - Must not contain '.', '/', '\\', ' ', ',', '`'
  // - Must start with a letter or an underscore
  private static final Pattern VALID_TABLE_NAME_PATTERN =
      Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  private ClickHouseUtils() {}

  /**
   * Checks whether the given table name is valid according to ClickHouse constraints.
   *
   * @param tableName the table name to check.
   * @throws ClickHouseResourceManagerException if the table name is invalid.
   */
  static void checkValidTableName(String tableName) {
    if (tableName.length() > MAX_TABLE_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Table name "
              + tableName
              + " cannot be longer than "
              + MAX_TABLE_NAME_LENGTH
              + " characters.");
    }
    if (!VALID_TABLE_NAME_PATTERN.matcher(tableName).matches()) {
      throw new IllegalArgumentException(
          "Table name "
              + tableName
              + " is not valid. It must contain only letters, numbers, and underscores and must start with a letter or an underscore.");
    }
  }
}
