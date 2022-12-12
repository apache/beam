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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DebeziumReadSchemaTransformProvider.class);
  private final Boolean isTest;
  private final Integer testLimitRecords;
  private final Integer testLimitMilliseconds;

  DebeziumReadSchemaTransformProvider() {
    this(false, null, null);
  }

  @VisibleForTesting
  DebeziumReadSchemaTransformProvider(Boolean isTest, Integer recordLimit, Integer timeLimitMs) {
    this.isTest = isTest;
    this.testLimitRecords = recordLimit;
    this.testLimitMilliseconds = timeLimitMs;
  }

  @Override
  protected @NonNull @Initialized Class<DebeziumReadSchemaTransformConfiguration>
      configurationClass() {
    return DebeziumReadSchemaTransformConfiguration.class;
  }

  @Override
  protected @NonNull @Initialized SchemaTransform from(
      DebeziumReadSchemaTransformConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            // TODO(pabloem): Test this behavior
            if (!Arrays.stream(Connectors.values())
                .map(Objects::toString)
                .collect(Collectors.toSet())
                .contains(configuration.getDatabase())) {
              throw new IllegalArgumentException(
                  "Unsupported dabase "
                      + configuration.getDatabase()
                      + ". Unable to select a JDBC driver for it. Supported Databases are: "
                      + String.join(
                          ", ",
                          Arrays.stream(Connectors.values())
                              .map(Object::toString)
                              .collect(Collectors.toList())));
            }
            Class<?> connectorClass =
                Objects.requireNonNull(Connectors.valueOf(configuration.getDatabase()))
                    .getConnector();
            DebeziumIO.ConnectorConfiguration connectorConfiguration =
                DebeziumIO.ConnectorConfiguration.create()
                    .withUsername(configuration.getUsername())
                    .withPassword(configuration.getPassword())
                    .withHostName(configuration.getHost())
                    .withPort(Integer.toString(configuration.getPort()))
                    .withConnectorClass(connectorClass);
            connectorConfiguration =
                connectorConfiguration
                    .withConnectionProperty("table.include.list", configuration.getTable())
                    .withConnectionProperty("include.schema.changes", "false")
                    .withConnectionProperty("database.server.name", "beam-pipeline-server")
                    .withConnectionProperty("database.dbname", "inventory")
                    .withConnectionProperty("database.include.list", "inventory");

            final List<String> debeziumConnectionProperties =
                configuration.getDebeziumConnectionProperties();
            if (debeziumConnectionProperties != null) {
              for (String connectionProperty : debeziumConnectionProperties) {
                String[] parts = connectionProperty.split("=", -1);
                String key = parts[0];
                String value = parts[1];
                connectorConfiguration.withConnectionProperty(key, value);
              }
            }

            DebeziumIO.Read<Row> readTransform =
                DebeziumIO.<Row>read().withConnectorConfiguration(connectorConfiguration);

            if (isTest) {
              readTransform =
                  readTransform
                      .withMaxNumberOfRecords(testLimitRecords)
                      .withMaxTimeToRun(testLimitMilliseconds);
            }

            // TODO(pabloem): Database connection issues can be debugged here.
            Schema recordSchema = readTransform.getRecordSchema();
            LOG.info(
                "Computed schema for table {} from {}: {}",
                configuration.getTable(),
                configuration.getDatabase(),
                recordSchema);
            SourceRecordMapper<Row> formatFn =
                KafkaConnectUtils.beamRowFromSourceRecordFn(recordSchema);
            readTransform =
                readTransform.withFormatFunction(formatFn).withCoder(RowCoder.of(recordSchema));

            return PCollectionRowTuple.of("output", input.getPipeline().apply(readTransform));
          }
        };
      }
    };
  }

  @Override
  public @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:debezium_read:v1";
  }

  @Override
  public @NonNull @Initialized List<@NonNull @Initialized String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @NonNull @Initialized List<@NonNull @Initialized String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @AutoValue
  public abstract static class DebeziumReadSchemaTransformConfiguration {
    public abstract String getUsername();

    public abstract String getPassword();

    public abstract String getHost();

    public abstract Integer getPort();

    public abstract String getTable();

    public abstract @NonNull String getDatabase();

    public abstract @Nullable List<String> getDebeziumConnectionProperties();

    public static Builder builder() {
      return new AutoValue_DebeziumReadSchemaTransformProvider_DebeziumReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUsername(String username);

      public abstract Builder setPassword(String password);

      public abstract Builder setHost(String host);

      public abstract Builder setPort(Integer port);

      public abstract Builder setDatabase(String database);

      public abstract Builder setTable(String table);

      public abstract Builder setDebeziumConnectionProperties(
          List<String> debeziumConnectionProperties);

      public abstract DebeziumReadSchemaTransformConfiguration build();
    }
  }
}
