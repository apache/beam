/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.io.debezium;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.db2.Db2Connector;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Enumeration of debezium connectors
 */
public enum Connectors {

    MYSQL("MySQL", MySqlConnector.class),
    POSTGRES("PostgreSQL", PostgresConnector.class),
    SQLSERVER("SQLServer", SqlServerConnector.class),
    ORACLE("Oracle", OracleConnector.class),
    DB2("DB2", Db2Connector.class),
    ;

    private final String name;
    private final Class<? extends SourceConnector> connector;

    Connectors(String name, Class<? extends SourceConnector> connector) {
        this.name = name;
        this.connector = connector;
    }

    /**
     * The name of this connector class
     */
    public String getName() {
        return name;
    }

    /**
     * Class connector to debezium
     */
    public Class<? extends SourceConnector> getConnector() {
        return connector;
    }

    /**
     * Returns a connector class corresponding to the given connector name
     *
     * @param connectorName
     *          The name of the connector. Ex.: MySQL
     * @return Connector enum representing the given connector name.
     */
    public static Connectors fromName(String connectorName) {
        for (Connectors connector: Connectors.values()) {
            if (connector.getName().equals(connectorName)) {
                return connector;
            }
        }
        throw new IllegalArgumentException("Cannot create enum from " + connectorName + " value!");
    }
}
