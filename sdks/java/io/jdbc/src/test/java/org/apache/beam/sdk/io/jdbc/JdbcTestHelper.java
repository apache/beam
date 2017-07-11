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
package org.apache.beam.sdk.io.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.common.TestRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains Test helper methods used by both Integration and Unit Tests in
 * {@link org.apache.beam.sdk.io.jdbc.JdbcIO}.
 */
class JdbcTestHelper {
  static final String READ_TABLE_NAME = "BEAM_TEST_READ";

  static String getWriteTableName() {
    return "BEAMTEST" + org.joda.time.Instant.now().getMillis();
  }

  static void createDataTable(
      DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }
    }
  }

  static void cleanUpDataTable(DataSource dataSource, String tableName)
      throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("drop table %s", tableName));
      }
    }
  }

  static class CreateTestRowOfNameAndId implements JdbcIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(
          resultSet.getInt("id"), resultSet.getString("name"));
    }
  }

  static class PrepareStatementFromTestRow
      implements JdbcIO.PreparedStatementSetter<TestRow> {
    @Override
    public void setParameters(TestRow element, PreparedStatement statement)
        throws SQLException {
      statement.setLong(1, element.id());
      statement.setString(2, element.name());
    }
  }

}
