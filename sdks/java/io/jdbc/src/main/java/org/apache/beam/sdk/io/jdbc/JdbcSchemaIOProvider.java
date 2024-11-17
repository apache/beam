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

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing JSON payloads with {@link
 * JdbcIO}.
 */
@Internal
@AutoService(SchemaIOProvider.class)
public class JdbcSchemaIOProvider implements SchemaIOProvider {

  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "jdbc";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder()
        .addStringField("driverClassName")
        .addStringField("jdbcUrl")
        .addStringField("username")
        .addStringField("password")
        .addNullableField("connectionProperties", FieldType.STRING)
        .addNullableField("connectionInitSqls", FieldType.iterable(FieldType.STRING))
        .addNullableField("readQuery", FieldType.STRING)
        .addNullableField("writeStatement", FieldType.STRING)
        .addNullableField("fetchSize", FieldType.INT16)
        .addNullableField("disableAutoCommit", FieldType.BOOLEAN)
        .addNullableField("outputParallelization", FieldType.BOOLEAN)
        .addNullableField("autosharding", FieldType.BOOLEAN)
        // Partitioning support. If you specify a partition column we will use that instead of
        // readQuery
        .addNullableField("partitionColumn", FieldType.STRING)
        .addNullableField("partitions", FieldType.INT16)
        .addNullableField("maxConnections", FieldType.INT16)
        .addNullableField("driverJars", FieldType.STRING)
        .build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public JdbcSchemaIO from(String location, Row configuration, @Nullable Schema dataSchema) {
    return new JdbcSchemaIO(location, configuration);
  }

  @Override
  public boolean requiresDataSchema() {
    return false;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  /** An abstraction to create schema aware IOs. */
  static class JdbcSchemaIO implements SchemaIO, Serializable {
    protected final Row config;
    protected final String location;

    JdbcSchemaIO(String location, Row config) {
      this.config = config;
      this.location = location;
    }

    @Override
    @SuppressWarnings("nullness") // need to fix core SDK, but in a separate change
    public @Nullable Schema schema() {
      return null;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin input) {

          // If we define a partition column we need to go a different route
          @Nullable
          String partitionColumn =
              config.getSchema().hasField("partitionColumn")
                  ? config.getString("partitionColumn")
                  : null;
          if (partitionColumn != null) {
            JdbcIO.ReadWithPartitions<Row, ?> readRows =
                JdbcIO.<Row>readWithPartitions()
                    .withDataSourceConfiguration(getDataSourceConfiguration())
                    .withTable(location)
                    .withPartitionColumn(partitionColumn)
                    .withRowOutput();
            @Nullable Short partitions = config.getInt16("partitions");
            if (partitions != null) {
              readRows = readRows.withNumPartitions(partitions);
            }

            @Nullable Short fetchSize = config.getInt16("fetchSize");
            if (fetchSize != null) {
              readRows = readRows.withFetchSize(fetchSize);
            }

            @Nullable Boolean disableAutoCommit = config.getBoolean("disableAutoCommit");
            if (disableAutoCommit != null) {
              readRows = readRows.withDisableAutoCommit(disableAutoCommit);
            }

            return input.apply(readRows);
          } else {

            @Nullable String readQuery = config.getString("readQuery");
            if (readQuery == null) {
              readQuery = String.format("SELECT * FROM %s", location);
            }

            JdbcIO.ReadRows readRows =
                JdbcIO.readRows()
                    .withDataSourceConfiguration(getDataSourceConfiguration())
                    .withQuery(readQuery);

            @Nullable Short fetchSize = config.getInt16("fetchSize");
            if (fetchSize != null) {
              readRows = readRows.withFetchSize(fetchSize);
            }

            @Nullable Boolean outputParallelization = config.getBoolean("outputParallelization");
            if (outputParallelization != null) {
              readRows = readRows.withOutputParallelization(outputParallelization);
            }

            @Nullable Boolean disableAutoCommit = config.getBoolean("disableAutoCommit");
            if (disableAutoCommit != null) {
              readRows = readRows.withDisableAutoCommit(disableAutoCommit);
            }

            return input.apply(readRows);
          }
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildWriter() {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          JdbcIO.Write<Row> writeRows =
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(getDataSourceConfiguration())
                  .withStatement(generateWriteStatement(input.getSchema()))
                  .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter());
          @Nullable Boolean autosharding = config.getBoolean("autosharding");
          if (autosharding != null && autosharding) {
            writeRows = writeRows.withAutoSharding();
          }
          return input.apply(writeRows);
        }
      };
    }

    protected JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
      JdbcIO.DataSourceConfiguration dataSourceConfiguration =
          JdbcIO.DataSourceConfiguration.create(
                  Preconditions.checkStateNotNull(config.getString("driverClassName")),
                  Preconditions.checkStateNotNull(config.getString("jdbcUrl")))
              .withUsername(config.getString("username"))
              .withPassword(config.getString("password"));

      @Nullable String connectionProperties = config.getString("connectionProperties");
      if (connectionProperties != null) {
        dataSourceConfiguration =
            dataSourceConfiguration.withConnectionProperties(connectionProperties);
      }

      @Nullable Iterable<String> connectionInitSqls = config.getIterable("connectionInitSqls");
      if (connectionInitSqls != null) {
        List<@Nullable String> initSqls =
            StreamSupport.stream(connectionInitSqls.spliterator(), false)
                .collect(Collectors.toList());
        dataSourceConfiguration = dataSourceConfiguration.withConnectionInitSqls(initSqls);
      }

      @Nullable Short maxConnections = config.getInt16("maxConnections");
      if (maxConnections != null) {
        dataSourceConfiguration =
            dataSourceConfiguration.withMaxConnections(maxConnections.intValue());
      }

      @Nullable String driverJars = config.getString("driverJars");
      if (driverJars != null) {
        dataSourceConfiguration = dataSourceConfiguration.withDriverJars(driverJars);
      }

      return dataSourceConfiguration;
    }

    private String generateWriteStatement(Schema schema) {
      @Nullable String configuredWriteStatement = config.getString("writeStatement");
      if (configuredWriteStatement != null) {
        return configuredWriteStatement;
      } else {
        StringBuilder writeStatement = new StringBuilder("INSERT INTO ");
        writeStatement.append(location);
        writeStatement.append(" VALUES(");
        for (int i = 0; i < schema.getFieldCount() - 1; i++) {
          writeStatement.append("?, ");
        }
        writeStatement.append("?)");
        return writeStatement.toString();
      }
    }
  }
}
