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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO.DataSourceConfiguration}. */
public class DataSourceConfigurationTest {

  private SnowflakeIO.DataSourceConfiguration configuration;
  private static final String SERVER_NAME = "account.snowflakecomputing.com";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  @Before
  public void setUp() {
    configuration = SnowflakeIO.DataSourceConfiguration.create();
  }

  @Test
  public void testSettingUrlWithBadPrefix() {
    Exception ex =
        assertThrows(IllegalArgumentException.class, () -> configuration.withUrl(SERVER_NAME));
    assertEquals(
        "url must have format: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
        ex.getMessage());
  }

  @Test
  public void testSettingUrlWithBadSuffix() {
    Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> configuration.withUrl("jdbc:snowflake://account"));
    assertEquals(
        "url must have format: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
        ex.getMessage());
  }

  @Test
  public void testSettingStringUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    configuration = configuration.withUrl(url).withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(url, configuration.getUrl());
  }

  @Test
  public void testSettingServerNameWithBadSuffix() {
    Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> configuration.withServerName("not.properly.ended"));
    assertEquals(
        "serverName must be in format <account_identifier>.snowflakecomputing.com",
        ex.getMessage());
  }

  @Test
  public void testSettingServerNameWithBadSuffixAndValueProvider() {
    configuration.withServerName(ValueProvider.StaticValueProvider.of("not.properly.ended"));
    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Server name or URL is required", ex.getMessage());
  }

  @Test
  public void testSettingStringServerName() {
    configuration = configuration.withServerName(SERVER_NAME).withOAuth("some-token");

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(SERVER_NAME, configuration.getServerName().get());
  }

  @Test
  public void testSettingStringDatabase() {
    String database = "dbname";
    configuration =
        configuration
            .withDatabase(database)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(database, configuration.getDatabase().get());
  }

  @Test
  public void testSettingStringWarehouse() {
    String warehouse = "warehouse";
    configuration =
        configuration
            .withWarehouse(warehouse)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(warehouse, configuration.getWarehouse().get());
  }

  @Test
  public void testSettingStringSchema() {
    String schema = "schema";
    configuration =
        configuration
            .withSchema(schema)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(schema, configuration.getSchema().get());
  }

  @Test
  public void testSettingStringRole() {
    String role = "role";
    configuration =
        configuration
            .withRole(role)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(role, configuration.getRole().get());
  }

  @Test
  public void testSettingStringAuthenticator() {
    String authenticator = "authenticator";
    configuration =
        configuration
            .withAuthenticator(authenticator)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(authenticator, configuration.getAuthenticator());
  }

  @Test
  public void testSettingStringPortNumber() {
    Integer portNumber = 1234;
    configuration =
        configuration
            .withPortNumber(portNumber)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(portNumber, configuration.getPortNumber());
  }

  @Test
  public void testSettingStringLoginTimeout() {
    Integer loginTimeout = 999;
    configuration =
        configuration
            .withLoginTimeout(loginTimeout)
            .withServerName(SERVER_NAME)
            .withUsernamePasswordAuth(USERNAME, PASSWORD);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(loginTimeout, configuration.getLoginTimeout());
  }

  @Test
  public void testDataSourceCreatedFromUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com?application=beam";
    configuration = configuration.withOAuth("some-token").withUrl(url);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerName() {
    configuration = configuration.withOAuth("some-token").withServerName(SERVER_NAME);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com?application=beam";
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerNameAndPort() {
    int portNumber = 1234;

    configuration =
        configuration
            .withOAuth("some-token")
            .withServerName(SERVER_NAME)
            .withPortNumber(portNumber);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com:1234?application=beam";
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testSettingUsernamePasswordAuth() {
    configuration =
        configuration.withUsernamePasswordAuth(USERNAME, PASSWORD).withServerName(SERVER_NAME);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertEquals(PASSWORD, configuration.getPassword().get());
  }

  @Test
  public void testSettingUsernamePasswordAuthWithMissingUsername() {
    configuration =
        configuration.withServerName(SERVER_NAME).withUsernamePasswordAuth(null, PASSWORD);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Missing credentials values. Please check your credentials", ex.getMessage());
  }

  @Test
  public void testSettingOAuth() {
    String token = "token";

    configuration = configuration.withOAuth(token).withServerName(SERVER_NAME);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(token, configuration.getOauthToken().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToKey() {
    String privateKeyPath = TestUtils.getValidEncryptedPrivateKeyPath(getClass());
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairPathAuth(USERNAME, privateKeyPath, keyPassphrase);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertNotNull(configuration.getRawPrivateKey());
    assertEquals(keyPassphrase, configuration.getPrivateKeyPassphrase().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToKeyAndInvalidKey() {
    String privateKeyPath = TestUtils.getInvalidPrivateKeyPath(getClass());
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairPathAuth(USERNAME, privateKeyPath, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertThat(ex.getMessage(), containsString("Can't create private key: "));
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToUnencryptedKeyAndPassphrase() {
    String privateKeyPath = TestUtils.getValidUnencryptedPrivateKeyPath(getClass());
    String keyPassphrase = "test-passphrase";

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairPathAuth(USERNAME, privateKeyPath, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertThat(
        ex.getMessage(),
        containsString(
            "The private key is unencrypted but private key key passphrase has been provided."));
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToEncryptedKeyAndNoPassphrase() {
    String privateKeyPath = TestUtils.getValidEncryptedPrivateKeyPath(getClass());

    configuration =
        configuration.withServerName(SERVER_NAME).withKeyPairPathAuth(USERNAME, privateKeyPath);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertThat(
        ex.getMessage(),
        containsString(
            "The private key is encrypted but no private key key passphrase has been provided."));
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToEncryptedKeyAndInvalidPassphrase() {
    String privateKeyPath = TestUtils.getValidEncryptedPrivateKeyPath(getClass());
    String keyPassphrase = "invalid-passphrase";

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairPathAuth(USERNAME, privateKeyPath, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertThat(ex.getMessage(), containsString("Can't create private key: "));
  }

  @Test
  public void testSettingKeyPairAuthWithProperPathToKeyAndMissingUsername() {
    String privateKeyPath = TestUtils.getValidEncryptedPrivateKeyPath(getClass());
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairPathAuth(null, privateKeyPath, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Missing credentials values. Please check your credentials", ex.getMessage());
  }

  @Test
  public void testSettingKeyPairAuthWithProperRawKey() throws IOException {
    String rawPrivateKey = TestUtils.getRawValidEncryptedPrivateKey(getClass());
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairRawAuth(USERNAME, rawPrivateKey, keyPassphrase);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertEquals(rawPrivateKey, configuration.getRawPrivateKey().get());
    assertEquals(keyPassphrase, configuration.getPrivateKeyPassphrase().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperRawEncryptedKeyWithoutHeaders() throws IOException {
    String rawPrivateKey =
        Arrays.stream(TestUtils.getRawValidEncryptedPrivateKey(getClass()).split("\n"))
            .filter(d -> !d.contains("---"))
            .collect(Collectors.joining("\n"));
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairRawAuth(USERNAME, rawPrivateKey, keyPassphrase);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertEquals(rawPrivateKey, configuration.getRawPrivateKey().get());
    assertEquals(keyPassphrase, configuration.getPrivateKeyPassphrase().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperRawUnencryptedKeyWithoutHeaders() throws IOException {
    String rawPrivateKey =
        Arrays.stream(TestUtils.getRawValidUnencryptedPrivateKey(getClass()).split("\n"))
            .filter(d -> !d.contains("---"))
            .collect(Collectors.joining("\n"));

    configuration =
        configuration.withServerName(SERVER_NAME).withKeyPairRawAuth(USERNAME, rawPrivateKey);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertEquals(rawPrivateKey, configuration.getRawPrivateKey().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperRawUnencryptedKey() throws IOException {
    String rawPrivateKey = TestUtils.getRawValidUnencryptedPrivateKey(getClass());

    configuration =
        configuration.withServerName(SERVER_NAME).withKeyPairRawAuth(USERNAME, rawPrivateKey);

    // Make sure DataSource can be built
    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());

    assertEquals(USERNAME, configuration.getUsername().get());
    assertEquals(rawPrivateKey, configuration.getRawPrivateKey().get());
  }

  @Test
  public void testSettingKeyPairAuthWithProperRawKeyAndMissingUsername() throws IOException {
    String rawPrivateKey = TestUtils.getRawValidEncryptedPrivateKey(getClass());
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairRawAuth(null, rawPrivateKey, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Missing credentials values. Please check your credentials", ex.getMessage());
  }

  @Test
  public void testSettingKeyPairAuthWithWrongPathToKey() {
    String privateKeyPath = "wrong/path/to/key/key.p8";
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    Exception ex =
        assertThrows(
            RuntimeException.class,
            () ->
                configuration
                    .withServerName(SERVER_NAME)
                    .withKeyPairPathAuth(USERNAME, privateKeyPath, keyPassphrase));
    assertEquals("Can't read private key from provided path", ex.getMessage());
  }

  @Test
  public void testSettingKeyPairAuthWithWrongKey() {
    String rawPrivateKey = "invalid_key";
    String keyPassphrase = TestUtils.getPrivateKeyPassphrase();

    configuration =
        configuration
            .withServerName(SERVER_NAME)
            .withKeyPairRawAuth(USERNAME, rawPrivateKey, keyPassphrase);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertThat(ex.getMessage(), containsString("Can't create private key: "));
  }

  @Test
  public void testSettingNonAuth() {
    configuration = configuration.withServerName(SERVER_NAME);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Missing credentials values. Please check your credentials", ex.getMessage());
  }

  @Test
  public void testMissingServerNameOrUrl() {
    configuration = configuration.withUsernamePasswordAuth(USERNAME, PASSWORD);

    Exception ex = assertThrows(RuntimeException.class, () -> configuration.buildDatasource());
    assertEquals("Server name or URL is required", ex.getMessage());
  }
}
