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
package org.apache.beam.io.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.io.debezium.DebeziumIO.ConnectorConfiguration;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test on the DebeziumIO. */
@RunWith(JUnit4.class)
public class DebeziumIOTest implements Serializable {

  private static final ConnectorConfiguration MYSQL_CONNECTOR_CONFIGURATION =
      ConnectorConfiguration.create()
          .withUsername("debezium")
          .withPassword("dbz")
          .withHostName("127.0.0.1")
          .withPort("3306")
          .withConnectorClass(MySqlConnector.class)
          .withConnectionProperty("database.server.id", "184054")
          .withConnectionProperty("database.server.name", "dbserver1")
          .withConnectionProperty(
              "database.history", KafkaSourceConsumerFn.DebeziumSDFDatabaseHistory.class.getName())
          .withConnectionProperty("include.schema.changes", "false");

  @Test
  public void testSourceMySqlConnectorValidConfiguration() {
    Map<String, String> configurationMap = MYSQL_CONNECTOR_CONFIGURATION.getConfigurationMap();

    Configuration debeziumConf = Configuration.from(configurationMap);
    Map<String, ConfigValue> validConfig = debeziumConf.validate(MySqlConnectorConfig.ALL_FIELDS);

    for (ConfigValue configValue : validConfig.values()) {
      assertTrue(configValue.errorMessages().isEmpty());
    }
  }

  @Test
  public void testSourceConnectorUsernamePassword() {
    String username = "debezium";
    String password = "dbz";
    ConnectorConfiguration configuration =
        MYSQL_CONNECTOR_CONFIGURATION.withUsername(username).withPassword(password);
    Map<String, String> configurationMap = configuration.getConfigurationMap();

    Configuration debeziumConf = Configuration.from(configurationMap);
    Map<String, ConfigValue> validConfig = debeziumConf.validate(MySqlConnectorConfig.ALL_FIELDS);

    for (ConfigValue configValue : validConfig.values()) {
      assertTrue(configValue.errorMessages().isEmpty());
    }
  }

  @Test
  public void testSourceConnectorNullPassword() {
    String username = "debezium";
    String password = null;

    assertThrows(
        IllegalArgumentException.class,
        () -> MYSQL_CONNECTOR_CONFIGURATION.withUsername(username).withPassword(password));
  }

  @Test
  public void testSourceConnectorNullUsernameAndPassword() {
    String username = null;
    String password = null;

    assertThrows(
        IllegalArgumentException.class,
        () -> MYSQL_CONNECTOR_CONFIGURATION.withUsername(username).withPassword(password));
  }

  @Test
  public void testReadWithStartOffsetStoresOffset() {
    Map<String, Object> offset = ImmutableMap.of("file", "mysql-bin.000003", "pos", 156L);
    DebeziumIO.Read<String> read =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withStartOffset(offset);
    assertEquals(offset, read.getStartOffset());
  }

  @Test
  public void testReadWithoutStartOffsetIsNull() {
    DebeziumIO.Read<String> read =
        DebeziumIO.<String>read().withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION);
    assertNull(read.getStartOffset());
  }

  @Test
  public void testReadWithNullStartOffsetThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DebeziumIO.<String>read()
                .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
                .withStartOffset(null));
  }

  @Test
  public void testGetInitialRestrictionUsesStartOffset() throws Exception {
    Map<String, Object> offset = ImmutableMap.of("file", "mysql-bin.000003", "pos", 156L);
    DebeziumIO.Read<String> spec =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withStartOffset(offset);
    KafkaSourceConsumerFn<String> fn = new KafkaSourceConsumerFn<>(MySqlConnector.class, spec);
    KafkaSourceConsumerFn.OffsetHolder restriction = fn.getInitialRestriction(null);
    assertEquals(offset, restriction.offset);
  }

  @Test
  public void testGetInitialRestrictionWithoutStartOffsetIsNull() throws Exception {
    DebeziumIO.Read<String> spec =
        DebeziumIO.<String>read().withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION);
    KafkaSourceConsumerFn<String> fn = new KafkaSourceConsumerFn<>(MySqlConnector.class, spec);
    KafkaSourceConsumerFn.OffsetHolder restriction = fn.getInitialRestriction(null);
    assertNull(restriction.offset);
  }

  // ---- OffsetRetainer tests -----------------------------------------------

  /** Minimal in-memory retainer used only in tests. */
  @SuppressWarnings("unused")
  private static class InMemoryOffsetRetainer implements OffsetRetainer {
    private final @Nullable Map<String, Object> loadResult;
    private @Nullable Map<String, Object> lastSaved;

    InMemoryOffsetRetainer(@Nullable Map<String, Object> loadResult) {
      this.loadResult = loadResult;
    }

    @Override
    public @Nullable Map<String, Object> loadOffset() {
      return loadResult;
    }

    @Override
    public void saveOffset(Map<String, Object> offset) {
      lastSaved = new HashMap<>(offset);
    }
  }

  @Test
  public void testWithOffsetRetainerStoresRetainer() {
    InMemoryOffsetRetainer retainer = new InMemoryOffsetRetainer(null);
    DebeziumIO.Read<String> read =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withOffsetRetainer(retainer);
    assertNotNull(read.getOffsetRetainer());
  }

  @Test
  public void testWithNullOffsetRetainerThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DebeziumIO.<String>read()
                .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
                .withOffsetRetainer(null));
  }

  @Test
  public void testGetInitialRestrictionUsesRetainerOffset() throws Exception {
    Map<String, Object> savedOffset = ImmutableMap.of("lsn", 28160840L);
    DebeziumIO.Read<String> spec =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withOffsetRetainer(new InMemoryOffsetRetainer(savedOffset));
    KafkaSourceConsumerFn<String> fn = new KafkaSourceConsumerFn<>(MySqlConnector.class, spec);
    KafkaSourceConsumerFn.OffsetHolder restriction = fn.getInitialRestriction(null);
    assertEquals(savedOffset, restriction.offset);
  }

  @Test
  public void testRetainerTakesPriorityOverWithStartOffset() throws Exception {
    Map<String, Object> retainerOffset = ImmutableMap.of("lsn", 99L);
    Map<String, Object> explicitOffset = ImmutableMap.of("lsn", 1L);
    DebeziumIO.Read<String> spec =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withStartOffset(explicitOffset)
            .withOffsetRetainer(new InMemoryOffsetRetainer(retainerOffset));
    KafkaSourceConsumerFn<String> fn = new KafkaSourceConsumerFn<>(MySqlConnector.class, spec);
    KafkaSourceConsumerFn.OffsetHolder restriction = fn.getInitialRestriction(null);
    assertEquals(retainerOffset, restriction.offset);
  }

  @Test
  public void testRetainerFallsBackToWithStartOffsetWhenLoadReturnsNull() throws Exception {
    Map<String, Object> explicitOffset = ImmutableMap.of("lsn", 1L);
    DebeziumIO.Read<String> spec =
        DebeziumIO.<String>read()
            .withConnectorConfiguration(MYSQL_CONNECTOR_CONFIGURATION)
            .withStartOffset(explicitOffset)
            // Retainer has no saved offset yet (returns null).
            .withOffsetRetainer(new InMemoryOffsetRetainer(null));
    KafkaSourceConsumerFn<String> fn = new KafkaSourceConsumerFn<>(MySqlConnector.class, spec);
    KafkaSourceConsumerFn.OffsetHolder restriction = fn.getInitialRestriction(null);
    assertEquals(explicitOffset, restriction.offset);
  }

  @Test
  public void testBuildExternalThrowsOnMalformedStartOffsetEntry() {
    DebeziumTransformRegistrar.ReadBuilder.Configuration config =
        new DebeziumTransformRegistrar.ReadBuilder.Configuration();
    config.setUsername("user");
    config.setPassword("pass");
    config.setHost("localhost");
    config.setPort("3306");
    config.setConnectorClass("MySQL");
    config.setStartOffset(Arrays.asList("lsn=100", "no-equals-sign"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DebeziumTransformRegistrar.ReadBuilder().buildExternal(config));
  }
}
