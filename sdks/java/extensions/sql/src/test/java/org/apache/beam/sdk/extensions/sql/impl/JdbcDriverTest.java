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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.beam.sdk.extensions.sql.meta.provider.BeamSqlTableProvider;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link JdbcDriver}. */
public class JdbcDriverTest {

  @Before
  public void before() throws Exception {
    Class.forName("org.apache.beam.sdk.extensions.sql.impl.JdbcDriver");
  }

  @Test
  public void testDriverManager_getDriver() throws Exception {
    Driver driver = DriverManager.getDriver(JdbcDriver.CONNECT_STRING_PREFIX);
    assertTrue(driver instanceof JdbcDriver);
  }

  @Test
  public void testDriverManager_simple() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    Statement statement = connection.createStatement();
    // SELECT 1 is a special case and does not reach the parser
    assertTrue(statement.execute("SELECT 1"));
  }

  @Test
  public void testDriverManager_parse() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    Statement statement = connection.createStatement();
    assertTrue(statement.execute("SELECT 'beam'"));
  }

  @Test
  public void testDriverManager_ddl() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);

    // Ensure no tables
    final DatabaseMetaData metadata = connection.getMetaData();
    ResultSet resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertFalse(resultSet.next());

    // Create tables
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("CREATE TABLE test (id INTEGER) TYPE 'text'"));

    // Ensure table test
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertTrue(resultSet.next());
    assertEquals("test", resultSet.getString("TABLE_NAME"));
    assertFalse(resultSet.next());

    // Create tables
    assertEquals(0, statement.executeUpdate("DROP TABLE test"));

    // Ensure no tables
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertFalse(resultSet.next());
  }

  @Test
  public void testInternalConnect_boundedTable() throws Exception {
    BeamSqlTableProvider tableProvider =
        new BeamSqlTableProvider(
            "test",
            ImmutableMap.of(
                "test",
                MockedBoundedTable.of(
                        Schema.FieldType.INT32, "id",
                        Schema.FieldType.STRING, "name")
                    .addRows(1, "first")));
    CalciteConnection connection = JdbcDriver.connect(tableProvider);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM test");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getInt("id"));
    assertEquals("first", resultSet.getString("name"));
    assertFalse(resultSet.next());
  }
}
