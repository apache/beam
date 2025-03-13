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
package org.apache.beam.it.gcp.datastream;

import com.google.cloud.datastream.v1.PostgresqlRdbms;
import com.google.cloud.datastream.v1.PostgresqlSchema;
import com.google.cloud.datastream.v1.PostgresqlSourceConfig;
import com.google.cloud.datastream.v1.PostgresqlTable;
import com.google.protobuf.MessageOrBuilder;
import java.util.List;
import java.util.Map;

/**
 * Client for PostgreSQL resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class PostgresqlSource extends JDBCSource {
  private final String database;
  private final String publication;
  private final String replicationSlot;

  PostgresqlSource(Builder builder) {
    super(builder);
    this.database = builder.database;
    this.publication = builder.publication;
    this.replicationSlot = builder.replicationSlot;
  }

  public static Builder builder(
      String hostname,
      String username,
      String password,
      int port,
      Map<String, List<String>> allowedTables,
      String database,
      String replicationSlot,
      String publication) {
    return new Builder(
        hostname, username, password, port, allowedTables, database, replicationSlot, publication);
  }

  @Override
  public SourceType type() {
    return SourceType.POSTGRESQL;
  }

  @Override
  public MessageOrBuilder config() {
    PostgresqlRdbms.Builder postgresqlRdbmsBuilder = PostgresqlRdbms.newBuilder();

    for (String schema : this.allowedTables().keySet()) {
      PostgresqlSchema.Builder postgresqlSchemaBuilder =
          PostgresqlSchema.newBuilder().setSchema(schema);
      for (String table : allowedTables().get(schema)) {
        postgresqlSchemaBuilder.addPostgresqlTables(PostgresqlTable.newBuilder().setTable(table));
      }
      postgresqlRdbmsBuilder.addPostgresqlSchemas(postgresqlSchemaBuilder);
    }
    return PostgresqlSourceConfig.newBuilder()
        .setIncludeObjects(postgresqlRdbmsBuilder)
        .setPublication(this.publication)
        .setReplicationSlot(this.replicationSlot);
  }

  public String database() {
    return this.database;
  }

  public static class Builder extends JDBCSource.Builder<PostgresqlSource> {
    private String database;
    private String publication;
    private String replicationSlot;

    public Builder(
        String hostname,
        String username,
        String password,
        int port,
        Map<String, List<String>> allowedTables,
        String database,
        String replicationSlot,
        String publication) {
      super(hostname, username, password, port, allowedTables);
      this.database = database;
      this.replicationSlot = replicationSlot;
      this.publication = publication;
    }

    @Override
    public PostgresqlSource build() {
      return new PostgresqlSource(this);
    }
  }
}
