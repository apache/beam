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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeServiceConfig;

/** Fake implementation of {@link SnowflakeService} used in tests. */
public class FakeSnowflakeServiceImpl implements SnowflakeService<SnowflakeServiceConfig> {

  @Override
  public void write(SnowflakeServiceConfig config) throws Exception {
    copyToTable(config);
  }

  @Override
  public String read(SnowflakeServiceConfig config) throws Exception {
    return copyIntoStage(config);
  }

  public String copyIntoStage(SnowflakeServiceConfig config) throws SQLException {
    String table = config.getTable();
    String query = config.getQuery();

    String stagingBucketDir = config.getStagingBucketDir();

    if (table != null) {
      writeToFile(stagingBucketDir, FakeSnowflakeDatabase.getElements(table));
    }
    if (query != null) {
      writeToFile(stagingBucketDir, FakeSnowflakeDatabase.runQuery(query));
    }

    return String.format("./%s/*", stagingBucketDir);
  }

  public void copyToTable(SnowflakeServiceConfig config) throws SQLException {
    List<String> filesList = config.getFilesList();
    String table = config.getTable();
    SnowflakeTableSchema tableSchema = config.getTableSchema();
    CreateDisposition createDisposition = config.getCreateDisposition();
    WriteDisposition writeDisposition = config.getWriteDisposition();

    List<String> rows = new ArrayList<>();
    for (String file : filesList) {
      rows.addAll(TestUtils.readGZIPFile(file.replace("'", "")));
    }

    prepareTableAccordingCreateDisposition(table, tableSchema, createDisposition);
    prepareTableAccordingWriteDisposition(table, writeDisposition);

    FakeSnowflakeDatabase.createTableWithElements(table, rows);
  }

  private void prepareTableAccordingCreateDisposition(
      String table, SnowflakeTableSchema tableSchema, CreateDisposition createDisposition)
      throws SQLException {
    switch (createDisposition) {
      case CREATE_NEVER:
        if (!FakeSnowflakeDatabase.isTableExist(table)) {
          throw new SnowflakeSQLException(
              null, "SQL compilation error: Table does not exist", table, 0);
        }
        break;
      case CREATE_IF_NEEDED:
        if (FakeSnowflakeDatabase.isTableExist(table)) {
          break;
        } else if (tableSchema == null) {
          throw new RuntimeException(
              "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");
        } else {
          FakeSnowflakeDatabase.createTable(table);
        }
        break;
    }
  }

  private void prepareTableAccordingWriteDisposition(
      String table, WriteDisposition writeDisposition) throws SQLException {
    switch (writeDisposition) {
      case TRUNCATE:
        FakeSnowflakeDatabase.truncateTable(table);
        break;
      case EMPTY:
        if (!FakeSnowflakeDatabase.isTableEmpty(table)) {
          throw new RuntimeException("Table is not empty. Aborting COPY with disposition EMPTY");
        }
        break;
      case APPEND:

      default:
        break;
    }
  }

  private void writeToFile(String stagingBucketNameTmp, List<String> rows) {
    Path filePath = Paths.get(String.format("./%s/table.csv.gz", stagingBucketNameTmp));
    try {
      Files.createDirectories(filePath.getParent());
      Files.createFile(filePath);
      Files.write(filePath, rows);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create files", e);
    }
  }
}
