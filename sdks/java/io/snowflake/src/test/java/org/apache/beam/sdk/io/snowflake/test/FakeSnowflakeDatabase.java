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
package org.apache.beam.sdk.io.snowflake.test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.SnowflakeSQLException;

/** Fake implementation of Snowflake warehouse used in test code. */
public class FakeSnowflakeDatabase implements Serializable {
  private static Map<String, List<String>> tables = new HashMap<>();

  private FakeSnowflakeDatabase() {
    tables = new HashMap<>();
  }

  public static void createTable(String table) {
    FakeSnowflakeDatabase.tables.put(table, Collections.emptyList());
  }

  public static List<String> getElements(String table) throws SnowflakeSQLException {
    if (!isTableExist(table)) {
      throw new SnowflakeSQLException(
          null, "SQL compilation error: Table does not exist", table, 0);
    }

    return FakeSnowflakeDatabase.tables.get(table);
  }

  public static List<String> runQuery(String query) throws SnowflakeSQLException {
    if (query.startsWith("SELECT * FROM ")) {
      String tableName = query.replace("SELECT * FROM ", "");
      return getElements(tableName);
    }
    throw new SnowflakeSQLException(null, "SQL compilation error: Invalid query", query, 0);
  }

  public static List<Long> getElementsAsLong(String table) throws SnowflakeSQLException {
    List<String> elements = getElements(table);
    return elements.stream().map(Long::parseLong).collect(Collectors.toList());
  }

  public static boolean isTableExist(String table) {
    return FakeSnowflakeDatabase.tables.containsKey(table);
  }

  public static boolean isTableEmpty(String table) {
    return FakeSnowflakeDatabase.tables.get(table).isEmpty();
  }

  public static void createTableWithElements(String table, List<String> rows) {
    FakeSnowflakeDatabase.tables.put(table, rows);
  }

  public static void clean() {
    FakeSnowflakeDatabase.tables = new HashMap<>();
  }

  public static void truncateTable(String table) {
    FakeSnowflakeDatabase.createTable(table);
  }
}
