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

import java.util.Properties;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO.Write;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for property merging in ClickHouseIO. */
@RunWith(JUnit4.class)
public class ClickHouseIOPropertyMergingTest {

  @Test
  public void testDeprecatedWriteExtractsPropertiesFromJdbcUrl() {
    String jdbcUrl =
        "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret&compress=true";
    String table = "test_table";

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table);

    Properties props = write.properties();
    assertEquals("admin", props.getProperty("user"));
    assertEquals("secret", props.getProperty("password"));
    assertEquals("true", props.getProperty("compress"));
  }

  @Test
  public void testWithPropertiesOverridesJdbcUrlProperties() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=old_secret";
    String table = "test_table";

    Properties newProps = new Properties();
    newProps.setProperty("password", "new_secret");
    newProps.setProperty("socket_timeout", "30000");

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(newProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user")); // From JDBC URL
    assertEquals("new_secret", finalProps.getProperty("password")); // Overridden
    assertEquals("30000", finalProps.getProperty("socket_timeout")); // Added
  }

  @Test
  public void testWithPropertiesPreservesExistingProperties() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&compress=true";
    String table = "test_table";

    Properties additionalProps = new Properties();
    additionalProps.setProperty("socket_timeout", "30000");

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(additionalProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user")); // Preserved from JDBC URL
    assertEquals("true", finalProps.getProperty("compress")); // Preserved from JDBC URL
    assertEquals("30000", finalProps.getProperty("socket_timeout")); // Added
  }

  @Test
  public void testMultipleWithPropertiesCalls() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    Properties props1 = new Properties();
    props1.setProperty("password", "secret1");
    props1.setProperty("compress", "true");

    Properties props2 = new Properties();
    props2.setProperty("password", "secret2"); // Override
    props2.setProperty("socket_timeout", "30000"); // Add

    @SuppressWarnings("deprecation")
    Write<?> write =
        ClickHouseIO.write(jdbcUrl, table).withProperties(props1).withProperties(props2);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user")); // From JDBC URL
    assertEquals("secret2", finalProps.getProperty("password")); // Last one wins
    assertEquals("true", finalProps.getProperty("compress")); // From first withProperties
    assertEquals("30000", finalProps.getProperty("socket_timeout")); // From second withProperties
  }

  @Test
  public void testNewWriteMethodWithProperties() {
    Properties props = new Properties();
    props.setProperty("user", "admin");
    props.setProperty("password", "secret");

    Write<?> write =
        ClickHouseIO.write("http://localhost:8123", "testdb", "test_table").withProperties(props);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user"));
    assertEquals("secret", finalProps.getProperty("password"));
  }

  @Test
  public void testEmptyPropertiesDoesNotAffectExisting() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret";
    String table = "test_table";

    Properties emptyProps = new Properties();

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(emptyProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user"));
    assertEquals("secret", finalProps.getProperty("password"));
  }

  @Test
  public void testPropertyPrecedenceOrder() {
    // Test that explicitly set properties take precedence over JDBC URL properties
    String jdbcUrl =
        "jdbc:clickhouse://localhost:8123/testdb?"
            + "user=url_user&"
            + "password=url_pass&"
            + "compress=false";
    String table = "test_table";

    Properties explicitProps = new Properties();
    explicitProps.setProperty("user", "explicit_user");
    explicitProps.setProperty("password", "explicit_pass");
    // compress not set, should remain from URL

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(explicitProps);

    Properties finalProps = write.properties();
    assertEquals("explicit_user", finalProps.getProperty("user"));
    assertEquals("explicit_pass", finalProps.getProperty("password"));
    assertEquals("false", finalProps.getProperty("compress"));
  }

  @Test
  public void testNullPropertyValueHandling() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    Properties props = new Properties();
    props.setProperty("user", "new_user");
    // Explicitly setting to empty string
    props.setProperty("password", "");

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(props);

    Properties finalProps = write.properties();
    assertEquals("new_user", finalProps.getProperty("user"));
    assertEquals("", finalProps.getProperty("password"));
  }

  @Test
  public void testComplexBackwardCompatibilityScenario() {
    // Real-world scenario: legacy JDBC URL with properties, plus additional configuration
    String legacyJdbcUrl =
        "jdbc:clickhouse://prod.example.com:8123/analytics?"
            + "user=analytics_ro&"
            + "password=legacy_pass&"
            + "compress=true&"
            + "socket_timeout=60000";

    String table = "events";

    // User wants to override credentials but keep other settings
    Properties newCredentials = new Properties();
    newCredentials.setProperty("user", "analytics_rw");
    newCredentials.setProperty("password", "new_secure_pass");
    newCredentials.setProperty("connection_timeout", "10000"); // Add new property

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(legacyJdbcUrl, table).withProperties(newCredentials);

    Properties finalProps = write.properties();

    // Overridden properties
    assertEquals("analytics_rw", finalProps.getProperty("user"));
    assertEquals("new_secure_pass", finalProps.getProperty("password"));

    // Preserved from JDBC URL
    assertEquals("true", finalProps.getProperty("compress"));
    assertEquals("60000", finalProps.getProperty("socket_timeout"));

    // Newly added
    assertEquals("10000", finalProps.getProperty("connection_timeout"));

    // Connection details should match parsed JDBC URL
    assertEquals("http://prod.example.com:8123", write.clickHouseUrl());
    assertEquals("analytics", write.database());
    assertEquals(table, write.table());
  }
}
