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

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link ClickHouseResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class ClickHouseResourceManagerIT {

  private ClickHouseResourceManager clickHouseResourceManager;

  @Before
  public void setUp() {
    clickHouseResourceManager = ClickHouseResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {

    String testTableName = "dummy_insert";
    Map<String, String> columns = new HashMap<>();
    columns.put("name", "String");
    columns.put("age", "Integer");
    boolean createTable = clickHouseResourceManager.createTable(testTableName, columns, null, null);

    assertThat(createTable).isTrue();

    List<Map<String, Object>> rows = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("name", "Alice");
    row1.put("age", 25);
    rows.add(row1);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("name", "Bob");
    row2.put("age", 30);
    rows.add(row2);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("name", "Charlie");
    row3.put("age", 28);
    rows.add(row3);

    boolean insertRows = clickHouseResourceManager.insertRows(testTableName, rows);

    assertThat(insertRows).isTrue();

    long count = clickHouseResourceManager.count(testTableName);

    assertThat(count).isEqualTo(3L);

    List<Map<String, Object>> fetchedRows = clickHouseResourceManager.fetchAll(testTableName);
    assertThat(fetchedRows).hasSize(3);
    assertThat(fetchedRows).containsExactlyElementsIn(rows);
  }
}
