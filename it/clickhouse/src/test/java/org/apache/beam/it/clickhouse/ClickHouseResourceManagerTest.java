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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.clickhouse.ClickHouseContainer;

@RunWith(JUnit4.class)
public class ClickHouseResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Connection mockConnection;

  @Mock private Statement mockStatement;

  @Mock private PreparedStatement mockPreparedStatement;

  @Mock private ResultSet mockResultSet;

  @Mock private ClickHouseContainer mockContainer;

  private ClickHouseResourceManager resourceManager;

  private static final String TEST_ID = "test-id";
  private static final String HOST = "localhost";
  private static final int CLICKHOUSE_PORT = 8123;
  private static final int MAPPED_PORT = 10000;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.initMocks(this); // This initializes the mocks
    when(mockContainer.createConnection("")).thenReturn(mockConnection);
    doReturn(mockContainer).when(mockContainer).withLogConsumer(any());

    resourceManager =
        new ClickHouseResourceManager(
            mockConnection, mockContainer, ClickHouseResourceManager.builder("test-id"));
  }

  @Test
  public void testTableExists_WhenTableExists() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("EXISTS TABLE test_table")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getInt(1)).thenReturn(1);

    assertTrue(resourceManager.tableExists("test_table"));
  }

  @Test
  public void testTableExists_WhenTableDoesNotExist() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("EXISTS TABLE test_table")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    assertFalse(resourceManager.tableExists("test_table"));
  }

  @Test
  public void testCreateTable_Success() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(anyString())).thenReturn(true);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    // 0 for the first does-exists call, and 1 for the table creation validation call
    when(mockResultSet.getInt(1)).thenReturn(0, 1);

    Map<String, String> columns = new HashMap<>();
    columns.put("id", "Int32");
    columns.put("name", "String");

    assertTrue(
        resourceManager.createTable(
            "test_table", columns, "MergeTree()", Collections.singletonList("id")));
  }

  @Test
  public void testInsertRows_Success() throws Exception {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("name", "Alice");
    rows.add(row1);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2);
    row2.put("name", "Bob");
    rows.add(row2);

    assertTrue(resourceManager.insertRows("test_table", rows));
    assertEquals(2, rows.size());
  }

  @Test
  public void testFetchAll_Success() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(anyInt())).thenReturn("Alice");
    when(mockResultSet.getMetaData().getColumnCount()).thenReturn(1);
    when(mockResultSet.getMetaData().getColumnName(1)).thenReturn("name");

    List<Map<String, Object>> result = resourceManager.fetchAll("test_table");
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));
  }

  @Test
  public void testCountRowsInTable() throws SQLException {
    String tableName = "test_table";
    ResultSet countRes = mock(ResultSet.class);
    when(countRes.next()).thenReturn(true);
    when(countRes.getLong(1)).thenReturn(10L);

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT count(*) FROM " + tableName)).thenReturn(countRes);
    when(mockConnection.createStatement()).thenReturn(statement);

    long rowCount = resourceManager.count(tableName);

    assertEquals(10L, rowCount, "Row count should be 10.");
  }

  @Test
  public void testInsertRowsWithInvalidData() {
    // Arrange
    String tableName = "test_table";
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", "string"); // Invalid data
    rows.add(row);

    // Act & Assert
    assertThrows(
        ClickHouseResourceManagerException.class,
        () -> resourceManager.insertRows(tableName, rows),
        "Should throw exception when inserting invalid data.");
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() throws SQLException {
    when(mockContainer.getHost()).thenReturn(HOST);
    when(mockContainer.getMappedPort(CLICKHOUSE_PORT)).thenReturn(MAPPED_PORT);

    assertThat(
            new ClickHouseResourceManager(
                    mockConnection, mockContainer, ClickHouseResourceManager.builder(TEST_ID))
                .getJdbcConnectionString())
        .matches("jdbc:clickhouse://" + HOST + ":" + MAPPED_PORT + "/default");
  }
}
