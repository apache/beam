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
package org.apache.beam.sdk.io.snowflake;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Implemenation of {@link org.apache.beam.sdk.io.snowflake.SnowflakeService} used in production.
 */
public class SnowflakeServiceImpl implements SnowflakeService {

  @Override
  public String copyIntoStage(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String query,
      String table,
      String integrationName,
      String stagingBucketName,
      String tmpDirName,
      SnowflakeCloudProvider cloudProvider)
      throws SQLException {

    String from;
    if (query != null) {
      // Query must be surrounded with brackets
      from = String.format("(%s)", query);
    } else {
      from = table;
    }

    String copyQuery =
        String.format(
            "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
            stagingBucketName, from, integrationName, CSV_QUOTE_CHAR_FOR_COPY);

    runStatement(copyQuery, getConnection(dataSourceProviderFn), null);

    return cloudProvider.transformSnowflakePathToCloudPath(stagingBucketName).concat("*");
  }

  private static void runStatement(String query, Connection connection, Consumer resultSetMethod)
      throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      if (resultSetMethod != null) {
        ResultSet resultSet = statement.executeQuery();
        resultSetMethod.accept(resultSet);
      } else {
        statement.execute();
      }
    } finally {
      statement.close();
      connection.close();
    }
  }

  private Connection getConnection(SerializableFunction<Void, DataSource> dataSourceProviderFn)
      throws SQLException {
    DataSource dataSource = dataSourceProviderFn.apply(null);
    return dataSource.getConnection();
  }
}
