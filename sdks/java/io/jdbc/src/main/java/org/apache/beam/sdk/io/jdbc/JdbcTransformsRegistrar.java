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

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Exposes {@link JdbcIO.Write} and {@link JdbcIO.ReadRows} as the external transforms for
 * cross-language usage.
 */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class JdbcTransformsRegistrar implements ExternalTransformRegistrar {

  public static final String READ_ROWS_URN = "beam:external:java:jdbc:read_rows:v1";
  public static final String WRITE_URN = "beam:external:java:jdbc:write:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(READ_ROWS_URN, ReadRowsBuilder.class, WRITE_URN, WriteBuilder.class);
  }

  private static class CrossLanguageConfiguration {
    String driverClassName;
    String jdbcUrl;
    String username;
    String password;
    String connectionProperties;
    Iterable<String> connectionInitSqls;

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

    protected JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
      JdbcIO.DataSourceConfiguration dataSourceConfiguration =
          JdbcIO.DataSourceConfiguration.create(driverClassName, jdbcUrl)
              .withUsername(username)
              .withPassword(password);

      if (connectionProperties != null) {
        dataSourceConfiguration =
            dataSourceConfiguration.withConnectionProperties(connectionProperties);
      }

      if (connectionInitSqls != null) {
        List<String> initSqls =
            StreamSupport.stream(connectionInitSqls.spliterator(), false)
                .collect(Collectors.toList());
        dataSourceConfiguration = dataSourceConfiguration.withConnectionInitSqls(initSqls);
      }
      return dataSourceConfiguration;
    }
  }

  public static class ReadRowsBuilder
      implements ExternalTransformBuilder<ReadRowsBuilder.Configuration, PBegin, PCollection<Row>> {

    public static class Configuration extends CrossLanguageConfiguration {
      private String query;
      private Integer fetchSize;
      private Boolean outputParallelization;

      public void setOutputParallelization(Boolean outputParallelization) {
        this.outputParallelization = outputParallelization;
      }

      public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
      }

      public void setQuery(String query) {
        this.query = query;
      }
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildExternal(Configuration configuration) {
      JdbcIO.ReadRows readRows =
          JdbcIO.readRows()
              .withDataSourceConfiguration(configuration.getDataSourceConfiguration())
              .withQuery(configuration.query);
      if (configuration.fetchSize != null) {
        readRows = readRows.withFetchSize(configuration.fetchSize);
      }
      if (configuration.outputParallelization != null) {
        readRows = readRows.withOutputParallelization(configuration.outputParallelization);
      }
      return readRows;
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<Row>, PDone> {

    public static class Configuration extends CrossLanguageConfiguration {
      private String statement;

      public void setStatement(String statement) {
        this.statement = statement;
      }
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
      DataSourceConfiguration dataSourceConfiguration = configuration.getDataSourceConfiguration();

      // TODO: BEAM-10396 use writeRows() when it's available
      return JdbcIO.<Row>write()
          .withDataSourceConfiguration(dataSourceConfiguration)
          .withStatement(configuration.statement)
          .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter());
    }
  }
}
