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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A POJO describing a SingleStoreDB {@link DataSource} by providing all properties needed to create
 * it.
 */
@AutoValue
public abstract class DataSourceConfiguration implements Serializable {
  abstract @Nullable String getEndpoint();

  abstract @Nullable String getUsername();

  abstract @Nullable String getPassword();

  abstract @Nullable String getDatabase();

  abstract @Nullable String getConnectionProperties();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setEndpoint(String endpoint);

    abstract Builder setUsername(String username);

    abstract Builder setPassword(String password);

    abstract Builder setDatabase(String database);

    abstract Builder setConnectionProperties(String connectionProperties);

    abstract DataSourceConfiguration build();
  }

  public static DataSourceConfiguration create(String endpoint) {
    checkNotNull(endpoint, "endpoint can not be null");
    return new AutoValue_DataSourceConfiguration.Builder().setEndpoint(endpoint).build();
  }

  public DataSourceConfiguration withUsername(String username) {
    checkNotNull(username, "username can not be null");
    return builder().setUsername(username).build();
  }

  public DataSourceConfiguration withPassword(String password) {
    checkNotNull(password, "password can not be null");
    return builder().setPassword(password).build();
  }

  public DataSourceConfiguration withDatabase(String database) {
    checkNotNull(database, "database can not be null");
    return builder().setDatabase(database).build();
  }

  /**
   * Sets the connection properties passed to driver.connect(...). Format of the string must be
   * [propertyName=property;]*
   *
   * <p>NOTE - The "user" and "password" properties can be add via {@link #withUsername(String)},
   * {@link #withPassword(String)}, so they do not need to be included here.
   *
   * <p>Full list of supported properties can be found here {@link <a
   * href="https://docs.singlestore.com/managed-service/en/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver.html#connection-string-parameters">...</a>}
   */
  public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
    checkNotNull(connectionProperties, "connectionProperties can not be null");
    return builder().setConnectionProperties(connectionProperties).build();
  }

  public static void populateDisplayData(
      @Nullable DataSourceConfiguration dataSourceConfiguration, DisplayData.Builder builder) {
    if (dataSourceConfiguration != null) {
      builder.addIfNotNull(DisplayData.item("endpoint", dataSourceConfiguration.getEndpoint()));
      builder.addIfNotNull(DisplayData.item("username", dataSourceConfiguration.getUsername()));
      builder.addIfNotNull(DisplayData.item("database", dataSourceConfiguration.getDatabase()));
      builder.addIfNotNull(
          DisplayData.item(
              "connectionProperties", dataSourceConfiguration.getConnectionProperties()));
    }
  }

  public DataSource getDataSource() {
    String endpoint = SingleStoreUtil.getRequiredArgument(getEndpoint(), "endpoint can not be null");
    String database = SingleStoreUtil.getArgumentWithDefault(getDatabase(), "");
    String connectionProperties = SingleStoreUtil.getArgumentWithDefault(getConnectionProperties(), "");
    connectionProperties += (connectionProperties.isEmpty() ? "" : ";") + "allowLocalInfile=TRUE";
    String username = getUsername();
    String password = getPassword();

    BasicDataSource basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName("com.singlestore.jdbc.Driver");
    basicDataSource.setUrl(String.format("jdbc:singlestore://%s/%s", endpoint, database));

    if (username != null) {
      basicDataSource.setUsername(username);
    }
    if (password != null) {
      basicDataSource.setPassword(password);
    }
    basicDataSource.setConnectionProperties(connectionProperties);

    return basicDataSource;
  }
}
