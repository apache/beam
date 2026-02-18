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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import org.apache.beam.sdk.io.clickhouse.ClickHouseJdbcUrlParser.ParsedJdbcUrl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ClickHouseJdbcUrlParser}. */
@RunWith(JUnit4.class)
public class ClickHouseJdbcUrlParserTest {

  @Test
  public void testBasicJdbcUrl() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/default";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
    assertTrue(parsed.getProperties().isEmpty());
  }

  @Test
  public void testJdbcUrlWithCustomDatabase() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
    assertTrue(parsed.getProperties().isEmpty());
  }

  @Test
  public void testJdbcUrlWithoutPort() {
    String jdbcUrl = "jdbc:clickhouse://localhost/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithoutDatabase() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithTrailingSlash() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithHttpPrefix() {
    String jdbcUrl = "jdbc:clickhouse:http://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithHttpsPrefix() {
    String jdbcUrl = "jdbc:clickhouse:https://localhost:8443/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithHttpsWithoutPort() {
    String jdbcUrl = "jdbc:clickhouse:https://localhost/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithSingleParameter() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb?user=admin";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
    assertEquals("admin", parsed.getProperties().getProperty("user"));
  }

  @Test
  public void testJdbcUrlWithMultipleParameters() {
    String jdbcUrl =
        "jdbc:clickhouse://localhost:8123/mydb?user=admin&password=secret&compress=true";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());

    Properties props = parsed.getProperties();
    assertEquals("admin", props.getProperty("user"));
    assertEquals("secret", props.getProperty("password"));
    assertEquals("true", props.getProperty("compress"));
  }

  @Test
  public void testJdbcUrlWithUrlEncodedParameters() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb?user=my%20user&password=p%40ssw0rd";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    Properties props = parsed.getProperties();
    assertEquals("my user", props.getProperty("user"));
    assertEquals("p@ssw0rd", props.getProperty("password"));
  }

  @Test
  public void testJdbcUrlWithParameterWithoutValue() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb?compress";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("true", parsed.getProperties().getProperty("compress"));
  }

  @Test
  public void testJdbcUrlShorthandCh() {
    String jdbcUrl = "jdbc:ch://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithRemoteHost() {
    String jdbcUrl = "jdbc:clickhouse://clickhouse.example.com:9000/production";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://clickhouse.example.com:9000", parsed.getClickHouseUrl());
    assertEquals("production", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithIpAddress() {
    String jdbcUrl = "jdbc:clickhouse://192.168.1.100:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://192.168.1.100:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithComplexQueryString() {
    String jdbcUrl =
        "jdbc:clickhouse://localhost:8123/mydb?"
            + "user=admin&password=secret&"
            + "socket_timeout=30000&"
            + "connection_timeout=10000&"
            + "compress=true";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    Properties props = parsed.getProperties();
    assertEquals("admin", props.getProperty("user"));
    assertEquals("secret", props.getProperty("password"));
    assertEquals("30000", props.getProperty("socket_timeout"));
    assertEquals("10000", props.getProperty("connection_timeout"));
    assertEquals("true", props.getProperty("compress"));
  }

  @Test
  public void testJdbcUrlCaseInsensitivePrefix() {
    String jdbcUrl = "JDBC:CLICKHOUSE://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testJdbcUrlWithDatabaseContainingUnderscore() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/my_database_name";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("my_database_name", parsed.getDatabase());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullJdbcUrl() {
    ClickHouseJdbcUrlParser.parse(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyJdbcUrl() {
    ClickHouseJdbcUrlParser.parse("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJdbcPrefix() {
    ClickHouseJdbcUrlParser.parse("jdbc:mysql://localhost:3306/mydb");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemeFtp() {
    ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:ftp://localhost:8123/mydb");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemeGopher() {
    ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:gopher://localhost:8123/mydb");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemeFile() {
    ClickHouseJdbcUrlParser.parse("jdbc:clickhouse:file://localhost:8123/mydb");
  }

  @Test
  public void testValidHttpSchemeExplicit() {
    String jdbcUrl = "jdbc:clickhouse:http://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testValidHttpsSchemeExplicit() {
    String jdbcUrl = "jdbc:clickhouse:https://localhost:8443/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testImplicitHttpScheme() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingHost() {
    ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://:8123/mydb");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedUrl() {
    ClickHouseJdbcUrlParser.parse("jdbc:clickhouse://localhost:invalid_port/mydb");
  }

  @Test
  public void testJdbcUrlWithoutJdbcPrefix() {
    // Should still work if user somehow passes URL without jdbc: prefix
    String jdbcUrl = "clickhouse://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testBackwardCompatibilityScenario() {
    // Simulating a real-world legacy JDBC URL
    String legacyJdbcUrl =
        "jdbc:clickhouse://prod-clickhouse.internal:8123/analytics?"
            + "user=analytics_user&"
            + "password=secure123&"
            + "compress=true&"
            + "socket_timeout=60000";

    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(legacyJdbcUrl);

    assertEquals("http://prod-clickhouse.internal:8123", parsed.getClickHouseUrl());
    assertEquals("analytics", parsed.getDatabase());

    Properties props = parsed.getProperties();
    assertEquals("analytics_user", props.getProperty("user"));
    assertEquals("secure123", props.getProperty("password"));
    assertEquals("true", props.getProperty("compress"));
    assertEquals("60000", props.getProperty("socket_timeout"));
  }

  @Test
  public void testJdbcUrlWithMultipleSlashesInPath() {
    // Edge case: malformed URL with multiple slashes
    String jdbcUrl = "jdbc:clickhouse://localhost:8123//mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    // URI parsing should normalize this
    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("/mydb", parsed.getDatabase()); // Will have leading slash
  }

  @Test
  public void testJdbcUrlWithQueryButNoDatabase() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123?user=admin";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
    assertEquals("admin", parsed.getProperties().getProperty("user"));
  }

  @Test
  public void testJdbcUrlWithEmptyQueryParameter() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb?user=&password=secret";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    Properties props = parsed.getProperties();
    assertEquals("", props.getProperty("user"));
    assertEquals("secret", props.getProperty("password"));
  }

  @Test
  public void testJdbcUrlWithSslParameter() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8443/mydb?ssl=true&user=admin";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
    assertEquals("admin", parsed.getProperties().getProperty("user"));
    assertEquals("true", parsed.getProperties().getProperty("ssl"));
  }

  @Test
  public void testJdbcUrlWithPort8443DefaultsToHttps() {
    String jdbcUrl = "jdbc:clickhouse://myhost:8443/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://myhost:8443", parsed.getClickHouseUrl());
    assertEquals("mydb", parsed.getDatabase());
  }

  @Test
  public void testClickHouseCloudUrl() {
    String jdbcUrl =
        "jdbc:clickhouse://someservice.clickhouse.cloud:8443/default?"
            + "user=default&password=secret&ssl=true&sslmode=NONE";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://someservice.clickhouse.cloud:8443", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
    assertEquals("default", parsed.getProperties().getProperty("user"));
    assertEquals("secret", parsed.getProperties().getProperty("password"));
    assertEquals("true", parsed.getProperties().getProperty("ssl"));
  }

  @Test
  public void testJdbcUrlPort8123DefaultsToHttp() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("http://localhost:8123", parsed.getClickHouseUrl());
  }

  @Test
  public void testHttpSchemeUpgradedToHttpsWhenSslTrue() {
    String jdbcUrl = "jdbc:clickhouse:http://localhost:8443/mydb?ssl=true";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    // Should upgrade http to https because ssl=true
    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
  }

  @Test
  public void testSslTrueTriggersHttps() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/mydb?ssl=true";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    // Should use https because ssl=true, even though port is 8123
    assertEquals("https://localhost:8123", parsed.getClickHouseUrl());
  }

  @Test
  public void testPort8443TriggersHttps() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8443/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    // Should use https because port is 8443
    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
  }

  @Test
  public void testClickHouseCloudUrlWithSsl() {
    String jdbcUrl =
        "jdbc:clickhouse://someservice.clickhouse.cloud:8443/default?"
            + "user=default&password=secret&ssl=true&sslmode=NONE";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://someservice.clickhouse.cloud:8443", parsed.getClickHouseUrl());
    assertEquals("default", parsed.getDatabase());
  }

  @Test
  public void testExplicitHttpsPreserved() {
    String jdbcUrl = "jdbc:clickhouse:https://localhost:8443/mydb";
    ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    assertEquals("https://localhost:8443", parsed.getClickHouseUrl());
  }
}
