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
package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * DebeziumIO Tester
 *
 * <p>
 *     Tests the three different Connectors.
 *     Requires each environment (MySQL, PostgreSQL, SQLServer) already set up.
 * </p>
 */
@RunWith(JUnit4.class)
public class DebeziumIOConnectorTest {
    /**
     * Debezium - MySQL connector Test
     *
     * <p>
     *     Tests that connector can actually connect to the database
     * </p>
     */
  @Test
  public void testDebeziumIOMySql() {
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  p.apply(DebeziumIO.<String>read().
              withConnectorConfiguration(
						DebeziumIO.ConnectorConfiguration.create()
							.withUsername("debezium")
							.withPassword("dbz")
							.withConnectorClass(MySqlConnector.class)
							.withHostName("127.0.0.1")
							.withPort("3306")
							.withConnectionProperty("database.server.id", "184054")
							.withConnectionProperty("database.server.name", "dbserver1")
							.withConnectionProperty("database.include.list", "inventory")
							.withConnectionProperty("include.schema.changes", "false"))
              .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
              .withCoder(StringUtf8Coder.of())
      );

	  p.run().waitUntilFinish();
  }

    /**
     * Debezium - PostgreSQL connector Test
     *
     * <p>
     *     Tests that connector can actually connect to the database
     * </p>
     */
    @Test
    public void testDebeziumIOPostgreSql() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername("postgres")
                                .withPassword("debezium")
                                .withConnectorClass(PostgresConnector.class)
                                .withHostName("127.0.0.1")
                                .withPort("5000")
                                .withConnectionProperty("database.dbname", "postgres")
                                .withConnectionProperty("database.server.name", "dbserver2")
                                .withConnectionProperty("schema.include.list", "inventory")
                                .withConnectionProperty("slot.name", "dbzslot2")
                                .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withCoder(StringUtf8Coder.of())
        );

        p.run().waitUntilFinish();
    }

    /**
     * Debezium - SQLServer connector Test
     *
     * <p>
     *     Tests that connector can actually connect to the database
     * </p>
     */
    @Test
    public void testDebeziumIOSqlSever() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername("sa")
                                .withPassword("Password!")
                                .withConnectorClass(SqlServerConnector.class)
                                .withHostName("127.0.0.1")
                                .withPort("1433")
                                .withConnectionProperty("database.dbname", "testDB")
                                .withConnectionProperty("database.server.name", "server1")
                                .withConnectionProperty("table.include.list", "dbo.customers")
                                .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withCoder(StringUtf8Coder.of())
        );

        p.run().waitUntilFinish();
    }

    @Test
    public void testDebeziumIODefaultOutputAsJson() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.readAsJson().
                withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername("sa")
                                .withPassword("Password!")
                                .withConnectorClass(SqlServerConnector.class)
                                .withHostName("127.0.0.1")
                                .withPort("1433")
                                .withConnectionProperty("database.dbname", "testDB")
                                .withConnectionProperty("database.server.name", "server1")
                                .withConnectionProperty("table.include.list", "dbo.customers")
                                .withConnectionProperty("include.schema.changes", "false")
                ));

        p.run().waitUntilFinish();
    }
}
