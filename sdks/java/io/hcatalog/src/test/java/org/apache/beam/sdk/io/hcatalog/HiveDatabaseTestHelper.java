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
package org.apache.beam.sdk.io.hcatalog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;

/** Helper for creating connection and test tables on hive database via JDBC driver. */
class HiveDatabaseTestHelper {
  private static Connection con;
  private static Statement stmt;

  HiveDatabaseTestHelper(
      String hiveHost,
      Integer hivePort,
      String hiveDatabase,
      String hiveUsername,
      String hivePassword)
      throws Exception {
    String hiveUrl = String.format("jdbc:hive2://%s:%s/%s", hiveHost, hivePort, hiveDatabase);
    con = DriverManager.getConnection(hiveUrl, hiveUsername, hivePassword);
    stmt = con.createStatement();
  }

  /** Create hive table. */
  String createHiveTable(String testIdentifier) throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName(testIdentifier);
    stmt.execute(" CREATE TABLE IF NOT EXISTS " + tableName + " (id STRING)");
    return tableName;
  }

  /** Delete hive table. */
  void dropHiveTable(String tableName) throws SQLException {
    stmt.execute(" DROP TABLE " + tableName);
  }

  void closeConnection() throws Exception {
    stmt.close();
    con.close();
  }
}
