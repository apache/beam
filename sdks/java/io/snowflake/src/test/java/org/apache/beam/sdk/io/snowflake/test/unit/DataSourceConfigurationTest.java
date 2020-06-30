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
package org.apache.beam.sdk.io.snowflake.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO.DataSourceConfiguration}. */
public class DataSourceConfigurationTest {

  private SnowflakeIO.DataSourceConfiguration configuration;

  @Before
  public void setUp() {
    configuration =
        SnowflakeIO.DataSourceConfiguration.create(
            new OAuthTokenSnowflakeCredentials("some-token"));
  }

  @Test
  public void testSettingUrlWithBadPrefix() {
    assertThrows(
        IllegalArgumentException.class,
        () -> configuration.withUrl("account.snowflakecomputing.com"));
  }

  @Test
  public void testSettingUrlWithBadSuffix() {
    assertThrows(
        IllegalArgumentException.class, () -> configuration.withUrl("jdbc:snowflake://account"));
  }

  @Test
  public void testSettingStringUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    configuration = configuration.withUrl(url);
    assertEquals(url, configuration.getUrl());
  }

  @Test
  public void testSettingServerNameWithBadSuffix() {
    assertThrows(
        IllegalArgumentException.class, () -> configuration.withServerName("not.properly.ended"));
  }

  @Test
  public void testSettingStringServerName() {
    String serverName = "account.snowflakecomputing.com";
    configuration = configuration.withServerName(serverName);
    assertEquals(serverName, configuration.getServerName());
  }

  @Test
  public void testSettingStringDatabase() {
    String database = "dbname";
    configuration = configuration.withDatabase(database);
    assertEquals(database, configuration.getDatabase());
  }

  @Test
  public void testSettingStringWarehouse() {
    String warehouse = "warehouse";
    configuration = configuration.withWarehouse(warehouse);
    assertEquals(warehouse, configuration.getWarehouse());
  }

  @Test
  public void testSettingStringSchema() {
    String schema = "schema";
    configuration = configuration.withSchema(schema);
    assertEquals(schema, configuration.getSchema());
  }

  @Test
  public void testSettingStringRole() {
    String role = "role";
    configuration = configuration.withRole(role);
    assertEquals(role, configuration.getRole());
  }

  @Test
  public void testSettingStringPortNumber() {
    Integer portNumber = 1234;
    configuration = configuration.withPortNumber(portNumber);
    assertEquals(portNumber, configuration.getPortNumber());
  }

  @Test
  public void testSettingStringLoginTimeout() {
    Integer loginTimeout = 999;
    configuration = configuration.withLoginTimeout(loginTimeout);
    assertEquals(loginTimeout, configuration.getLoginTimeout());
  }

  @Test
  public void testDataSourceCreatedFromUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com?application=beam";
    configuration = configuration.withUrl(url);

    DataSource dataSource = configuration.buildDatasource();

    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerName() {
    String serverName = "account.snowflakecomputing.com";
    configuration = configuration.withServerName(serverName);

    DataSource dataSource = configuration.buildDatasource();

    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com?application=beam";
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerNameAndPort() {
    String serverName = "account.snowflakecomputing.com";
    int portNumber = 1234;

    configuration = configuration.withServerName(serverName);
    configuration = configuration.withPortNumber(portNumber);

    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com:1234?application=beam";
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }
}
