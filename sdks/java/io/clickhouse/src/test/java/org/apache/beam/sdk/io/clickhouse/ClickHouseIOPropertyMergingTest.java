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
import static org.junit.Assert.fail;

import java.util.Properties;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO.Write;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for property conflict detection in ClickHouseIO. */
@RunWith(JUnit4.class)
public class ClickHouseIOPropertyMergingTest {

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedWriteExtractsPropertiesFromJdbcUrl() {
    String jdbcUrl =
        "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret&compress=true";
    String table = "test_table";

    Write<?> write = ClickHouseIO.write(jdbcUrl, table);

    Properties props = write.properties();
    assertEquals("admin", props.getProperty("user"));
    assertEquals("secret", props.getProperty("password"));
    assertEquals("true", props.getProperty("compress"));
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testWithPropertiesConflictThrows() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=old_secret";
    String table = "test_table";

    Properties conflictingProps = new Properties();
    conflictingProps.setProperty("password", "new_secret"); // Conflicts!

    ClickHouseIO.write(jdbcUrl, table).withProperties(conflictingProps); // Should throw
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithPropertiesNoConflictWhenSameValue() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret";
    String table = "test_table";

    Properties sameProps = new Properties();
    sameProps.setProperty("user", "admin"); // Same value - OK
    sameProps.setProperty("password", "secret"); // Same value - OK

    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(sameProps);

    assertEquals("admin", write.properties().getProperty("user"));
    assertEquals("secret", write.properties().getProperty("password"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithPropertiesAddsNewPropertiesWithoutConflict() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    Properties additionalProps = new Properties();
    additionalProps.setProperty("socket_timeout", "30000"); // New property - OK
    additionalProps.setProperty("compress", "true"); // New property - OK

    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(additionalProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user"));
    assertEquals("30000", finalProps.getProperty("socket_timeout"));
    assertEquals("true", finalProps.getProperty("compress"));
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
  @SuppressWarnings("deprecation")
  public void testEmptyPropertiesDoesNotAffectExisting() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret";
    String table = "test_table";

    Properties emptyProps = new Properties();

    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(emptyProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user"));
    assertEquals("secret", finalProps.getProperty("password"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithPropertiesConflictHasDetailedMessage() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?compress=false";
    String table = "test_table";

    Properties conflictingProps = new Properties();
    conflictingProps.setProperty("compress", "true"); // Different value

    try {
      ClickHouseIO.write(jdbcUrl, table).withProperties(conflictingProps);
      fail("Expected IllegalArgumentException for property conflict");
    } catch (IllegalArgumentException e) {
      // Verify error message is helpful
      assertTrue(e.getMessage().contains("compress"));
      assertTrue(e.getMessage().contains("false"));
      assertTrue(e.getMessage().contains("true"));
      assertTrue(e.getMessage().contains("conflict"));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testMultipleWithPropertiesCallsWithConflict() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?password=original";
    String table = "test_table";

    Properties props1 = new Properties();
    props1.setProperty("compress", "true"); // New property - OK

    Properties props2 = new Properties();
    props2.setProperty("password", "secret2"); // Conflicts with JDBC URL!

    ClickHouseIO.write(jdbcUrl, table).withProperties(props1).withProperties(props2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMultipleWithPropertiesCallsWithoutConflict() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    Properties props1 = new Properties();
    props1.setProperty("compress", "true"); // New property - OK

    Properties props2 = new Properties();
    props2.setProperty("socket_timeout", "30000"); // New property - OK

    Write<?> write =
        ClickHouseIO.write(jdbcUrl, table).withProperties(props1).withProperties(props2);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user")); // From JDBC URL
    assertEquals("true", finalProps.getProperty("compress")); // From first withProperties
    assertEquals("30000", finalProps.getProperty("socket_timeout")); // From second withProperties
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testCannotOverrideJdbcUrlProperties() {
    // This test verifies the NEW behavior: conflicts are not allowed
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=url_user&password=url_pass";
    String table = "test_table";

    Properties conflictingProps = new Properties();
    conflictingProps.setProperty("user", "explicit_user"); // Conflict!

    ClickHouseIO.write(jdbcUrl, table).withProperties(conflictingProps); // Should throw
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCanAddPropertiesToJdbcUrlWithoutConflict() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    Properties additionalProps = new Properties();
    additionalProps.setProperty("password", "secret"); // New - no conflict
    additionalProps.setProperty("compress", "true"); // New - no conflict

    Write<?> write = ClickHouseIO.write(jdbcUrl, table).withProperties(additionalProps);

    Properties finalProps = write.properties();
    assertEquals("admin", finalProps.getProperty("user"));
    assertEquals("secret", finalProps.getProperty("password"));
    assertEquals("true", finalProps.getProperty("compress"));
  }
}
