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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class which exposes an implementation {@link #read} and a Debezium configuration.
 *
 * <h3>Quick Overview</h3>
 *
 * <p>This class lets Beam users connect to their existing Debezium implementations in an easy way.
 *
 * <p>Any Kafka connector supported by Debezium should work fine with this IO.
 *
 * <p>The following connectors were tested and worked well in some simple scenarios:
 *
 * <ul>
 *   <li>MySQL
 *   <li>PostgreSQL
 *   <li>SQLServer
 *   <li>DB2
 * </ul>
 *
 * <h3>Usage example</h3>
 *
 * <p>Connect to a Debezium - MySQL database and run a Pipeline
 *
 * <pre>
 *     private static final ConnectorConfiguration mySqlConnectorConfig = ConnectorConfiguration
 *             .create()
 *             .withUsername("uname")
 *             .withPassword("pwd123")
 *             .withHostName("127.0.0.1")
 *             .withPort("3306")
 *             .withConnectorClass(MySqlConnector.class)
 *             .withConnectionProperty("database.server.id", "184054")
 *             .withConnectionProperty("database.server.name", "serverid")
 *             .withConnectionProperty("database.include.list", "dbname")
 *             .withConnectionProperty("database.history", DebeziumSDFDatabaseHistory.class.getName())
 *             .withConnectionProperty("include.schema.changes", "false");
 *
 *      PipelineOptions options = PipelineOptionsFactory.create();
 *      Pipeline p = Pipeline.create(options);
 *      p.apply(DebeziumIO.read()
 *               .withConnectorConfiguration(mySqlConnectorConfig)
 *               .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
 *       ).setCoder(StringUtf8Coder.of());
 *       p.run().waitUntilFinish();
 * </pre>
 *
 * <p>In this example we are using {@link KafkaSourceConsumerFn.DebeziumSDFDatabaseHistory} to
 * handle the Database history.
 *
 * <h3>Dependencies</h3>
 *
 * <p>User may work with any of the supported Debezium Connectors above mentioned
 *
 * <p>See <a href="https://debezium.io/documentation/reference/1.3/connectors/index.html">Debezium
 * Connectors</a> for more info.
 */
@SuppressWarnings({"nullness"})
public class DebeziumIO {
  private static final Logger LOG = LoggerFactory.getLogger(DebeziumIO.class);

  /**
   * Read data from a Debezium source.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_DebeziumIO_Read.Builder<T>().build();
  }

  /**
   * Read data from Debezium source and convert a Kafka {@link
   * org.apache.kafka.connect.source.SourceRecord} into a JSON string using {@link
   * org.apache.beam.io.debezium.SourceRecordJson.SourceRecordJsonMapper} as default function
   * mapper.
   *
   * @return Reader object of String.
   */
  public static Read<String> readAsJson() {
    return new AutoValue_DebeziumIO_Read.Builder<String>()
        .setFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        .setCoder(StringUtf8Coder.of())
        .build();
  }

  /** Disallow construction of utility class. */
  private DebeziumIO() {}

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private static final long serialVersionUID = 1L;

    abstract @Nullable ConnectorConfiguration getConnectorConfiguration();

    abstract @Nullable SourceRecordMapper<T> getFormatFunction();

    abstract @Nullable Integer getMaxNumberOfRecords();

    abstract @Nullable Long getMaxTimeToRun();

    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectorConfiguration(ConnectorConfiguration config);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setFormatFunction(SourceRecordMapper<T> mapperFn);

      abstract Builder<T> setMaxNumberOfRecords(Integer maxNumberOfRecords);

      abstract Builder<T> setMaxTimeToRun(Long miliseconds);

      abstract Read<T> build();
    }

    /**
     * Applies the given configuration to the connector. It cannot be null.
     *
     * @param config Configuration to be used within the connector.
     * @return PTransform {@link #read}
     */
    public Read<T> withConnectorConfiguration(final ConnectorConfiguration config) {
      checkArgument(config != null, "config can not be null");
      return toBuilder().setConnectorConfiguration(config).build();
    }

    /**
     * Applies a {@link SourceRecordMapper} to the connector. It cannot be null.
     *
     * @param mapperFn the mapper function to be used on each {@link
     *     org.apache.kafka.connect.source.SourceRecord}.
     * @return PTransform {@link #read}
     */
    public Read<T> withFormatFunction(SourceRecordMapper<T> mapperFn) {
      checkArgument(mapperFn != null, "mapperFn can not be null");
      return toBuilder().setFormatFunction(mapperFn).build();
    }

    /**
     * Applies a {@link Coder} to the connector. It cannot be null
     *
     * @param coder The Coder to be used over the data.
     * @return PTransform {@link #read}
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    /**
     * Once the specified number of records has been reached, it will stop fetching them. The value
     * can be null (default) which means it will not stop.
     *
     * @param maxNumberOfRecords The maximum number of records to be fetched before stop.
     * @return PTransform {@link #read}
     */
    public Read<T> withMaxNumberOfRecords(Integer maxNumberOfRecords) {
      return toBuilder().setMaxNumberOfRecords(maxNumberOfRecords).build();
    }

    /**
     * Once the connector has run for the determined amount of time, it will stop. The value can be
     * null (default) which means it will not stop. This parameter is mainly intended for testing.
     *
     * @param miliseconds The maximum number of miliseconds to run before stopping the connector.
     * @return PTransform {@link #read}
     */
    public Read<T> withMaxTimeToRun(Long miliseconds) {
      return toBuilder().setMaxTimeToRun(miliseconds).build();
    }

    protected Schema getRecordSchema() {
      KafkaSourceConsumerFn<T> fn =
          new KafkaSourceConsumerFn<>(
              getConnectorConfiguration().getConnectorClass().get(),
              getFormatFunction(),
              getMaxNumberOfRecords());
      fn.register(
          new KafkaSourceConsumerFn.OffsetTracker(
              new KafkaSourceConsumerFn.OffsetHolder(null, null, 0)));

      Map<String, String> connectorConfig =
          Maps.newHashMap(getConnectorConfiguration().getConfigurationMap());
      connectorConfig.put("snapshot.mode", "schema_only");
      SourceRecord sampledRecord =
          fn.getOneRecord(getConnectorConfiguration().getConfigurationMap());
      fn.reset();
      Schema keySchema =
          sampledRecord.keySchema() != null
              ? KafkaConnectUtils.beamSchemaFromKafkaConnectSchema(sampledRecord.keySchema())
              : Schema.builder().build();
      Schema valueSchema =
          KafkaConnectUtils.beamSchemaFromKafkaConnectSchema(sampledRecord.valueSchema());

      return Schema.builder()
          .addFields(valueSchema.getFields())
          .setOptions(
              Schema.Options.builder()
                  .setOption(
                      "primaryKeyColumns",
                      Schema.FieldType.array(Schema.FieldType.STRING),
                      keySchema.getFields().stream()
                          .map(Schema.Field::getName)
                          .collect(Collectors.toList())))
          .build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(
              Create.of(Lists.newArrayList(getConnectorConfiguration().getConfigurationMap()))
                  .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
          .apply(
              ParDo.of(
                  new KafkaSourceConsumerFn<>(
                      getConnectorConfiguration().getConnectorClass().get(),
                      getFormatFunction(),
                      getMaxNumberOfRecords(),
                      getMaxTimeToRun())))
          .setCoder(getCoder());
    }
  }

  /** A POJO describing a Debezium configuration. */
  @AutoValue
  public abstract static class ConnectorConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    abstract @Nullable ValueProvider<Class<?>> getConnectorClass();

    abstract @Nullable ValueProvider<String> getHostName();

    abstract @Nullable ValueProvider<String> getPort();

    abstract @Nullable ValueProvider<String> getUsername();

    abstract @Nullable ValueProvider<String> getPassword();

    abstract @Nullable ValueProvider<SourceConnector> getSourceConnector();

    abstract @Nullable ValueProvider<Map<String, String>> getConnectionProperties();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectorClass(ValueProvider<Class<?>> connectorClass);

      abstract Builder setHostName(ValueProvider<String> hostname);

      abstract Builder setPort(ValueProvider<String> port);

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setConnectionProperties(
          ValueProvider<Map<String, String>> connectionProperties);

      abstract Builder setSourceConnector(ValueProvider<SourceConnector> sourceConnector);

      abstract ConnectorConfiguration build();
    }

    /**
     * Creates a ConnectorConfiguration.
     *
     * @return {@link ConnectorConfiguration}
     */
    public static ConnectorConfiguration create() {
      return new AutoValue_DebeziumIO_ConnectorConfiguration.Builder()
          .setConnectionProperties(ValueProvider.StaticValueProvider.of(new HashMap<>()))
          .build();
    }

    /**
     * Applies the connectorClass to be used to connect to your database.
     *
     * <p>Currently supported connectors are:
     *
     * <ul>
     *   <li>{@link io.debezium.connector.mysql.MySqlConnector}
     *   <li>{@link io.debezium.connector.postgresql.PostgresConnector}
     *   <li>{@link io.debezium.connector.sqlserver.SqlServerConnector }
     * </ul>
     *
     * @param connectorClass Any of the supported connectors.
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withConnectorClass(Class<?> connectorClass) {
      checkArgument(connectorClass != null, "connectorClass can not be null");
      return withConnectorClass(ValueProvider.StaticValueProvider.of(connectorClass));
    }

    /**
     * Sets the connectorClass to be used to connect to your database. It cannot be null.
     *
     * <p>Currently supported connectors are:
     *
     * <ul>
     *   <li>{@link io.debezium.connector.mysql.MySqlConnector}
     *   <li>{@link io.debezium.connector.postgresql.PostgresConnector}
     *   <li>{@link io.debezium.connector.sqlserver.SqlServerConnector }
     * </ul>
     *
     * @param connectorClass (as ValueProvider)
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withConnectorClass(ValueProvider<Class<?>> connectorClass) {
      checkArgument(connectorClass != null, "connectorClass can not be null");
      return builder().setConnectorClass(connectorClass).build();
    }

    /**
     * Sets the host name to be used on the database. It cannot be null.
     *
     * @param hostName The hostname of your database.
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withHostName(String hostName) {
      checkArgument(hostName != null, "hostName can not be null");
      return withHostName(ValueProvider.StaticValueProvider.of(hostName));
    }

    /**
     * Sets the host name to be used on the database. It cannot be null.
     *
     * @param hostName The hostname of your database (as ValueProvider).
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withHostName(ValueProvider<String> hostName) {
      checkArgument(hostName != null, "hostName can not be null");
      return builder().setHostName(hostName).build();
    }

    /**
     * Sets the port on which your database is listening. It cannot be null.
     *
     * @param port The port to be used to connect to your database (as ValueProvider).
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withPort(String port) {
      checkArgument(port != null, "port can not be null");
      return withPort(ValueProvider.StaticValueProvider.of(port));
    }

    /**
     * Sets the port on which your database is listening. It cannot be null.
     *
     * @param port The port to be used to connect to your database.
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withPort(ValueProvider<String> port) {
      checkArgument(port != null, "port can not be null");
      return builder().setPort(port).build();
    }

    /**
     * Sets the username to connect to your database. It cannot be null.
     *
     * @param username Database username
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    /**
     * Sets the username to connect to your database. It cannot be null.
     *
     * @param username (as ValueProvider).
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withUsername(ValueProvider<String> username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    /**
     * Sets the password to connect to your database. It cannot be null.
     *
     * @param password Database password
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    /**
     * Sets the password to connect to your database. It cannot be null.
     *
     * @param password (as ValueProvider).
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withPassword(ValueProvider<String> password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * Sets a custom property to be used within the connection to your database.
     *
     * <p>You may use this to set special configurations such as:
     *
     * <ul>
     *   <li>slot.name
     *   <li>database.dbname
     *   <li>database.server.id
     *   <li>...
     * </ul>
     *
     * @param connectionProperties Properties (Key, Value) Map
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withConnectionProperties(
        Map<String, String> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return withConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties));
    }

    /**
     * Sets a custom property to be used within the connection to your database.
     *
     * <p>You may use this to set special configurations such as:
     *
     * <ul>
     *   <li>slot.name
     *   <li>database.dbname
     *   <li>database.server.id
     *   <li>...
     * </ul>
     *
     * @param connectionProperties (as ValueProvider).
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withConnectionProperties(
        ValueProvider<Map<String, String>> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    /**
     * Sets a custom property to be used within the connection to your database.
     *
     * <p>You may use this to set special configurations such as:
     *
     * <ul>
     *   <li>slot.name
     *   <li>database.dbname
     *   <li>database.server.id
     *   <li>...
     * </ul>
     *
     * @param key Property name
     * @param value Property value
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withConnectionProperty(String key, String value) {
      checkArgument(key != null, "key can not be null");
      checkArgument(value != null, "value can not be null");
      checkArgument(
          getConnectionProperties().get() != null, "connectionProperties can not be null");

      ConnectorConfiguration config = builder().build();
      config.getConnectionProperties().get().putIfAbsent(key, value);
      return config;
    }

    /**
     * Sets the {@link SourceConnector} to be used. It cannot be null.
     *
     * @param sourceConnector Any supported connector
     * @return {@link ConnectorConfiguration}
     */
    public ConnectorConfiguration withSourceConnector(SourceConnector sourceConnector) {
      checkArgument(sourceConnector != null, "sourceConnector can not be null");
      return withSourceConnector(ValueProvider.StaticValueProvider.of(sourceConnector));
    }

    public ConnectorConfiguration withSourceConnector(
        ValueProvider<SourceConnector> sourceConnector) {
      checkArgument(sourceConnector != null, "sourceConnector can not be null");
      return builder().setSourceConnector(sourceConnector).build();
    }

    /**
     * Configuration Map Getter.
     *
     * @return Configuration Map.
     */
    public Map<String, String> getConfigurationMap() {
      HashMap<String, String> configuration = new HashMap<>();

      configuration.computeIfAbsent(
          "connector.class", k -> getConnectorClass().get().getCanonicalName());
      configuration.computeIfAbsent("database.hostname", k -> getHostName().get());
      configuration.computeIfAbsent("database.port", k -> getPort().get());
      configuration.computeIfAbsent("database.user", k -> getUsername().get());
      configuration.computeIfAbsent("database.password", k -> getPassword().get());

      for (Map.Entry<String, String> entry : getConnectionProperties().get().entrySet()) {
        configuration.computeIfAbsent(entry.getKey(), k -> entry.getValue());
      }

      // Set default Database History impl. if not provided
      configuration.computeIfAbsent(
          "database.history",
          k -> KafkaSourceConsumerFn.DebeziumSDFDatabaseHistory.class.getName());

      String stringProperties = Joiner.on('\n').withKeyValueSeparator(" -> ").join(configuration);
      LOG.debug("---------------- Connector configuration: {}", stringProperties);

      return configuration;
    }
  }
}
