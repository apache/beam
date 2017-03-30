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
package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * These options can be used by a test connecting to a postgres database to configure their
 * connection.
 */
public interface PostgresTestOptions extends TestPipelineOptions {
    @Description("Server name for postgres server (host name/ip address)")
    @Default.String("postgres-server-name")
    String getPostgresServerName();
    void setPostgresServerName(String value);

    @Description("Username for postgres server")
    @Default.String("postgres-username")
    String getPostgresUsername();
    void setPostgresUsername(String value);

    // Note that passwords are not as secure an authentication as other methods, and used here for
    // a test environment only.
    @Description("Password for postgres server")
    @Default.String("postgres-password")
    String getPostgresPassword();
    void setPostgresPassword(String value);

    @Description("Database name for postgres server")
    @Default.String("postgres-database-name")
    String getPostgresDatabaseName();
    void setPostgresDatabaseName(String value);

    @Description("Port for postgres server")
    @Default.Integer(0)
    Integer getPostgresPort();
    void setPostgresPort(Integer value);

    @Description("Whether the postgres server uses SSL")
    @Default.Boolean(true)
    Boolean getPostgresSsl();
    void setPostgresSsl(Boolean value);
}
