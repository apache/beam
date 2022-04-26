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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.io.debezium.DebeziumIO.ConnectorConfiguration;
import org.apache.kafka.common.config.ConfigValue;
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
          .withConnectionProperty("database.include.list", "inventory")
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
}
