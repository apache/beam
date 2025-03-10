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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ClickHouseUtilsTest {

  @Test
  void testValidTableName() {
    String validTableName = "valid_table_name";

    // Test should pass without any exception
    assertDoesNotThrow(() -> ClickHouseUtils.checkValidTableName(validTableName));
  }

  @Test
  void testTableNameTooLong() {
    // Create a table name of 207 characters manually (using StringBuilder for Java 8)
    StringBuilder tableNameTooLong = new StringBuilder();
    for (int i = 0; i < 207; i++) {
      tableNameTooLong.append("t");
    }

    // Test should throw IllegalArgumentException due to length constraint
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseUtils.checkValidTableName(tableNameTooLong.toString()));

    assertEquals(
        "Table name " + tableNameTooLong + " cannot be longer than 206 characters.",
        exception.getMessage());
  }

  @Test
  void testTableNameWithInvalidCharacters() {
    String invalidTableName = "invalid.table,name";

    // Test should throw IllegalArgumentException due to invalid characters in the name
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseUtils.checkValidTableName(invalidTableName));

    assertEquals(
        "Table name "
            + invalidTableName
            + " is not valid. It must contain only letters, numbers, and underscores and must start with a letter or an underscore.",
        exception.getMessage());
  }

  @Test
  void testTableNameStartingWithNonLetter() {
    String invalidTableName = "1invalid_table_name";

    // Test should throw IllegalArgumentException because table name starts with a non-letter
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseUtils.checkValidTableName(invalidTableName));

    assertEquals(
        "Table name "
            + invalidTableName
            + " is not valid. It must contain only letters, numbers, and underscores and must start with a letter or an underscore.",
        exception.getMessage());
  }

  @Test
  void testTableNameWithUnderscoreAtStart() {
    String validTableName = "_valid_table_name";

    // Test should pass without any exception
    assertDoesNotThrow(() -> ClickHouseUtils.checkValidTableName(validTableName));
  }
}
