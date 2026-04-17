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

import org.apache.beam.sdk.io.clickhouse.ClickHouseIO.Write;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for JDBC URL backward compatibility. */
@RunWith(JUnit4.class)
public class ClickHouseIOJdbcBackwardCompatibilityTest {

  @Test
  public void testDeprecatedWriteMethodWithBasicJdbcUrl() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb";
    String table = "test_table";

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table);

    assertEquals("http://localhost:8123", write.clickHouseUrl());
    assertEquals("testdb", write.database());
    assertEquals(table, write.table());
  }

  @Test
  public void testDeprecatedWriteMethodWithParameters() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin&password=secret";
    String table = "test_table";

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table);

    assertEquals("http://localhost:8123", write.clickHouseUrl());
    assertEquals("testdb", write.database());
    assertEquals(table, write.table());
    assertEquals("admin", write.properties().getProperty("user"));
    assertEquals("secret", write.properties().getProperty("password"));
  }

  @Test
  public void testDeprecatedWriteMethodPreservesDefaults() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb";
    String table = "test_table";

    @SuppressWarnings("deprecation")
    Write<?> write = ClickHouseIO.write(jdbcUrl, table);

    // Verify defaults are set
    assertEquals(ClickHouseIO.DEFAULT_MAX_INSERT_BLOCK_SIZE, write.maxInsertBlockSize());
    assertEquals(ClickHouseIO.DEFAULT_MAX_RETRIES, write.maxRetries());
    assertEquals(ClickHouseIO.DEFAULT_INITIAL_BACKOFF, write.initialBackoff());
    assertEquals(ClickHouseIO.DEFAULT_MAX_CUMULATIVE_BACKOFF, write.maxCumulativeBackoff());
    assertTrue(write.insertDeduplicate());
    assertTrue(write.insertDistributedSync());
  }

  @Test
  public void testNewWriteMethodEquivalence() {
    String jdbcUrl = "jdbc:clickhouse://localhost:8123/testdb?user=admin";
    String table = "test_table";

    // Old way (deprecated)
    @SuppressWarnings("deprecation")
    Write<?> oldWrite = ClickHouseIO.write(jdbcUrl, table);

    // New way
    Write<?> newWrite =
        ClickHouseIO.write("http://localhost:8123", "testdb", table)
            .withProperties(oldWrite.properties());

    // Should produce equivalent configurations
    assertEquals(oldWrite.clickHouseUrl(), newWrite.clickHouseUrl());
    assertEquals(oldWrite.database(), newWrite.database());
    assertEquals(oldWrite.table(), newWrite.table());
    assertEquals(
        oldWrite.properties().getProperty("user"), newWrite.properties().getProperty("user"));
  }
}
