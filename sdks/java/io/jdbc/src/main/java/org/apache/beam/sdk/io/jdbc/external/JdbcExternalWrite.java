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
package org.apache.beam.sdk.io.jdbc.external;

import com.google.auto.service.AutoService;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link JdbcIO.Write} as an external transform for cross-language usage. */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class JdbcExternalWrite implements ExternalTransformRegistrar {

  public static final String URN = "beam:external:java:jdbc:write:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, JdbcExternalWrite.Builder.class);
  }

  /** Parameters class to expose the Write transform to an external SDK. */
  public static class Configuration {
    private String statement;
    private String driverClassName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String connectionProperties;
    private Iterable<String> connectionInitSqls;

    public void setStatement(String statement) {
      this.statement = statement;
    }

    public void setDriverClassName(String driverClassName) {
      this.driverClassName = driverClassName;
    }

    public void setJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setConnectionProperties(String connectionProperties) {
      this.connectionProperties = connectionProperties;
    }

    public void setConnectionInitSqls(Iterable<String> connectionInitSqls) {
      this.connectionInitSqls = connectionInitSqls;
    }
  }

  public static class Builder
      implements ExternalTransformBuilder<
          JdbcExternalWrite.Configuration, PCollection<Row>, PDone> {
    @Override
    public PTransform<PCollection<Row>, PDone> buildExternal(
        JdbcExternalWrite.Configuration configuration) {
      DataSourceConfiguration dataSourceConfiguration =
          DataSourceConfiguration.create(configuration.driverClassName, configuration.jdbcUrl)
              .withUsername(configuration.username)
              .withPassword(configuration.password)
              .withConnectionProperties(configuration.connectionProperties);

      if (configuration.connectionInitSqls != null) {
        List<String> connectionInitSqls =
            StreamSupport.stream(configuration.connectionInitSqls.spliterator(), false)
                .collect(Collectors.toList());
        dataSourceConfiguration =
            dataSourceConfiguration.withConnectionInitSqls(connectionInitSqls);
      }

      JdbcIO.PreparedStatementSetter<Row> preparedStatementSetter = new PrepareStatementXlangRow();

      return JdbcIO.<Row>write()
          .withDataSourceConfiguration(dataSourceConfiguration)
          .withStatement(configuration.statement)
          .withPreparedStatementSetter(new XlangPreparedStatementSetter());
    }

    private static class XlangPreparedStatementSetter
        implements JdbcIO.PreparedStatementSetter<Row> {
      @Override
      public void setParameters(Row row, PreparedStatement statement) throws SQLException {
        List<Schema.Field> fieldTypes = row.getSchema().getFields();
        for (int i = 0; i < fieldTypes.size(); ++i) {
          Schema.FieldType type = fieldTypes.get(i).getType();
          if (Schema.FieldType.BYTE.equals(type)) {
            statement.setByte(i + 1, row.getByte(i));
          } else if (Schema.FieldType.INT16.equals(type)) {
            statement.setShort(i + 1, row.getInt16(i));
          } else if (Schema.FieldType.INT32.equals(type)) {
            statement.setInt(i + 1, row.getInt32(i));
          } else if (Schema.FieldType.INT64.equals(type)) {
            statement.setLong(i + 1, row.getInt64(i));
          } else if (Schema.FieldType.FLOAT.equals(type)) {
            statement.setFloat(i + 1, row.getFloat(i));
          } else if (Schema.FieldType.DOUBLE.equals(type)) {
            statement.setDouble(i + 1, row.getDouble(i));
          } else if (Schema.FieldType.DECIMAL.equals(type)) {
            statement.setBigDecimal(i + 1, row.getDecimal(i));
          } else if (Schema.FieldType.BOOLEAN.equals(type)) {
            statement.setBoolean(i + 1, row.getBoolean(i));
          } else if (Schema.FieldType.STRING.equals(type)) {
            statement.setString(i + 1, row.getString(i));
          } else if (Schema.FieldType.BYTES.equals(type)) {
            statement.setBytes(i + 1, row.getBytes(i));
          } else if (Schema.FieldType.DATETIME.equals(type)) {
            statement.setDate(i + 1, new Date(row.getDateTime(i).toDateTime().getMillis()));
          } else {
            throw new IllegalArgumentException(String.format("Unknown FieldType: %s", type));
          }
        }
      }
    }
  }
}
