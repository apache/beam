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

import org.apache.kafka.connect.source.SourceConnector;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Enumeration of debezium connectors. */
public enum Connectors {
  MYSQL("MySQL", "io.debezium.connector.mysql.MySqlConnector"),
  POSTGRES("PostgreSQL", "io.debezium.connector.postgresql.PostgresConnector"),
  SQLSERVER("SQLServer", "io.debezium.connector.sqlserver.SqlServerConnector"),
  ORACLE("Oracle", "io.debezium.connector.oracle.OracleConnector"),
  DB2("DB2", "io.debezium.connector.db2.Db2Connector"),
  ;
  private final String name;
  private final String connector;

  Connectors(String name, String connector) {
    this.name = name;
    this.connector = connector;
  }

  /** The name of this connector class. */
  public String getName() {
    return name;
  }

  /** Class connector to debezium. */
  public @NonNull Class<? extends SourceConnector> getConnector() {
    Class<? extends SourceConnector> connectorClass = null;
    try {
      connectorClass = (Class<? extends SourceConnector>) Class.forName(this.connector);
    } catch (ClassCastException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to resolve class %s to use as Debezium connector.", this.connector));
    }
    return connectorClass;
  }

  /**
   * Returns a connector class corresponding to the given connector name.
   *
   * @param connectorName The name of the connector. Ex.: MySQL
   * @return Connector enum representing the given connector name.
   */
  public static Connectors fromName(String connectorName) {
    for (Connectors connector : Connectors.values()) {
      if (connector.getName().equals(connectorName)) {
        return connector;
      }
    }
    throw new IllegalArgumentException("Cannot create enum from " + connectorName + " value!");
  }
}
