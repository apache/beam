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
import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Fake implementation of {@link org.apache.beam.sdk.io.snowflake.SnowflakeService} used in tests.
 */
public class FakeSnowflakeServiceImpl implements SnowflakeService {

  @Override
  public String copyIntoStage(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String query,
      String table,
      String integrationName,
      String stagingBucketName)
      throws SQLException {

    if (table != null) {
      writeToFile(stagingBucketName, FakeSnowflakeDatabase.getElements(table));
    }
    if (query != null) {
      writeToFile(stagingBucketName, FakeSnowflakeDatabase.runQuery(query));
    }

    return String.format("./%s/*", stagingBucketName);
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
