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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Abstract Parameters class to expose the Jdbc transforms to an external SDK. */
public abstract class CrossLanguageConfiguration {
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
