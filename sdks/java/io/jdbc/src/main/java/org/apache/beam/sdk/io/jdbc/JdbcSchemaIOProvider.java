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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

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
        .addNullableField("outputParallelization", FieldType.BOOLEAN)
        .build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public JdbcSchemaIO from(String location, Row configuration, Schema dataSchema) {
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
    public Schema schema() {
      return null;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin input) {
          String readQuery;
          if (config.getString("readQuery") != null) {
            readQuery = config.getString("readQuery");
          } else {
            readQuery = String.format("SELECT * FROM %s", location);
          }

          JdbcIO.ReadRows readRows =
              JdbcIO.readRows()
                  .withDataSourceConfiguration(getDataSourceConfiguration())
                  .withQuery(readQuery);

          if (config.getInt16("fetchSize") != null) {
            readRows = readRows.withFetchSize(config.getInt16("fetchSize"));
          }
          if (config.getBoolean("outputParallelization") != null) {
            readRows =
                readRows.withOutputParallelization(config.getBoolean("outputParallelization"));
          }
          return input.apply(readRows);
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildWriter() {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          // TODO: BEAM-10396 use writeRows() when it's available
          return input.apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(getDataSourceConfiguration())
                  .withStatement(generateWriteStatement(input.getSchema()))
                  .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter()));
        }
      };
    }

    protected JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
      @Nullable Iterable<String> connectionInitSqls = config.getIterable("connectionInitSqls");

      JdbcIO.DataSourceConfiguration dataSourceConfiguration =
          JdbcIO.DataSourceConfiguration.create(
                  config.getString("driverClassName"), config.getString("jdbcUrl"))
              .withUsername(config.getString("username"))
              .withPassword(config.getString("password"));

      if (config.getString("connectionProperties") != null) {
        dataSourceConfiguration =
            dataSourceConfiguration.withConnectionProperties(
                config.getString("connectionProperties"));
      }

      if (connectionInitSqls != null) {
        List<String> initSqls =
            StreamSupport.stream(connectionInitSqls.spliterator(), false)
                .collect(Collectors.toList());
        dataSourceConfiguration = dataSourceConfiguration.withConnectionInitSqls(initSqls);
      }
      return dataSourceConfiguration;
    }

    private String generateWriteStatement(Schema schema) {
      if (config.getString("writeStatement") != null) {
        return config.getString("writeStatement");
      } else {
        String writeStatement = String.format("INSERT INTO %s VALUES(", location);
        for (int i = 0; i < schema.getFieldCount() - 1; i++) {
          writeStatement += "?, ";
        }
        return writeStatement + "?)";
      }
    }
  }
}
