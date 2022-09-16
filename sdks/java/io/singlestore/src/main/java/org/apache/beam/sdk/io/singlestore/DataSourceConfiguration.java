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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;

@AutoValue
public abstract class DataSourceConfiguration implements Serializable {
  abstract @Nullable ValueProvider<String> getEndpoint();

  abstract @Nullable ValueProvider<String> getUsername();

  abstract @Nullable ValueProvider<String> getPassword();

  abstract @Nullable ValueProvider<String> getDatabase();

  abstract @Nullable ValueProvider<String> getConnectionProperties();

  abstract DataSourceConfiguration.Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract DataSourceConfiguration.Builder setEndpoint(ValueProvider<String> endpoint);

    abstract DataSourceConfiguration.Builder setUsername(ValueProvider<String> username);

    abstract DataSourceConfiguration.Builder setPassword(ValueProvider<String> password);

    abstract DataSourceConfiguration.Builder setDatabase(ValueProvider<String> database);

    abstract DataSourceConfiguration.Builder setConnectionProperties(
        ValueProvider<String> connectionProperties);

    abstract DataSourceConfiguration build();
  }

  public static DataSourceConfiguration create(String endpoint) {
    checkNotNull(endpoint, "endpoint can not be null");
    return create(ValueProvider.StaticValueProvider.of(endpoint));
  }

  public static DataSourceConfiguration create(ValueProvider<String> endpoint) {
    checkNotNull(endpoint, "endpoint can not be null");
    return new AutoValue_DataSourceConfiguration.Builder().setEndpoint(endpoint).build();
  }

  public DataSourceConfiguration withUsername(String username) {
    checkNotNull(username, "username can not be null");
    return withUsername(ValueProvider.StaticValueProvider.of(username));
  }

  public DataSourceConfiguration withUsername(ValueProvider<String> username) {
    checkNotNull(username, "username can not be null");
    return builder().setUsername(username).build();
  }

  public DataSourceConfiguration withPassword(String password) {
    checkNotNull(password, "password can not be null");
    return withPassword(ValueProvider.StaticValueProvider.of(password));
  }

  public DataSourceConfiguration withPassword(ValueProvider<String> password) {
    checkNotNull(password, "password can not be null");
    return builder().setPassword(password).build();
  }

  public DataSourceConfiguration withDatabase(String database) {
    checkNotNull(database, "database can not be null");
    return withDatabase(ValueProvider.StaticValueProvider.of(database));
  }

  public DataSourceConfiguration withDatabase(ValueProvider<String> database) {
    checkNotNull(database, "database can not be null");
    return builder().setDatabase(database).build();
  }

  public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
    checkNotNull(connectionProperties, "connectionProperties can not be null");
    return withConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties));
  }

  public DataSourceConfiguration withConnectionProperties(
      ValueProvider<String> connectionProperties) {
    checkNotNull(connectionProperties, "connectionProperties can not be null");
    return builder().setConnectionProperties(connectionProperties).build();
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder.addIfNotNull(DisplayData.item("endpoint", getEndpoint()));
    builder.addIfNotNull(DisplayData.item("username", getUsername()));
    builder.addIfNotNull(DisplayData.item("database", getDatabase()));
    builder.addIfNotNull(DisplayData.item("connectionProperties", getConnectionProperties()));
  }

  public DataSource getDataSource() throws SQLException {
    checkArgument(getEndpoint() != null && getEndpoint().get() != null, "endpoint can not be null");

    String database =
        (getDatabase() != null && getDatabase().get() != null) ? getDatabase().get() : "";
    String connectionProperties = (getConnectionProperties() != null && getConnectionProperties().get() != null) ?
        String.format("%s;allowLocalInfile=TRUE", getConnectionProperties().get()) :
        "allowLocalInfile=TRUE";

    BasicDataSource basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName("com.singlestore.jdbc.Driver");
    basicDataSource.setUrl(
        String.format("jdbc:singlestore://%s/%s", getEndpoint().get(), database));

    if (getUsername() != null && getUsername().get() != null) {
      basicDataSource.setUsername(getUsername().get());
    }
    if (getPassword() != null && getPassword().get() != null) {
      basicDataSource.setPassword(getPassword().get());
    }
    basicDataSource.setConnectionProperties(connectionProperties);

    return basicDataSource;
  }
}
