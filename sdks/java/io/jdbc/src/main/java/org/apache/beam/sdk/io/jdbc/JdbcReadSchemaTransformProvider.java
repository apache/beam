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

import static org.apache.beam.sdk.io.jdbc.JdbcUtil.JDBC_DRIVER_MAP;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * An implementation of {@link org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider} for
 * reading from JDBC connections using {@link org.apache.beam.sdk.io.jdbc.JdbcIO}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JdbcReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration> {

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:jdbc_read:v1";
  }

  @Override
  public String description() {
    return baseDescription("JDBC")
        + "\n"
        + "This transform can be used to read from a JDBC source using either a given JDBC driver jar "
        + "and class name, or by using one of the default packaged drivers given a `jdbc_type`.\n"
        + "\n"
        + "#### Using a default driver\n"
        + "\n"
        + "This transform comes packaged with drivers for several popular JDBC distributions. The following "
        + "distributions can be declared as the `jdbc_type`: "
        + JDBC_DRIVER_MAP.keySet().toString().replaceAll("[\\[\\]]", "")
        + ".\n"
        + "\n"
        + "For example, reading a MySQL source using a SQL query: ::"
        + "\n"
        + "    - type: ReadFromJdbc\n"
        + "      config:\n"
        + "        jdbc_type: mysql\n"
        + "        url: \"jdbc:mysql://my-host:3306/database\"\n"
        + "        query: \"SELECT * FROM table\"\n"
        + "\n"
        + "\n"
        + "**Note**: See the following transforms which are built on top of this transform and simplify "
        + "this logic for several popular JDBC distributions:\n\n"
        + " - ReadFromMySql\n"
        + " - ReadFromPostgres\n"
        + " - ReadFromOracle\n"
        + " - ReadFromSqlServer\n"
        + "\n"
        + "#### Declaring custom JDBC drivers\n"
        + "\n"
        + "If reading from a JDBC source not listed above, or if it is necessary to use a custom driver not "
        + "packaged with Beam, one must define a JDBC driver and class name.\n"
        + "\n"
        + "For example, reading a MySQL source table: ::"
        + "\n"
        + "    - type: ReadFromJdbc\n"
        + "      config:\n"
        + "        driver_jars: \"path/to/some/jdbc.jar\"\n"
        + "        driver_class_name: \"com.mysql.jdbc.Driver\"\n"
        + "        url: \"jdbc:mysql://my-host:3306/database\"\n"
        + "        table: \"my-table\"\n"
        + "\n"
        + "#### Connection Properties\n"
        + "\n"
        + "Connection properties are properties sent to the Driver used to connect to the JDBC source. For example, "
        + "to set the character encoding to UTF-8, one could write: ::\n"
        + "\n"
        + "    - type: ReadFromJdbc\n"
        + "      config:\n"
        + "        connectionProperties: \"characterEncoding=UTF-8;\"\n"
        + "        ...\n"
        + "All properties should be semi-colon-delimited (e.g. \"key1=value1;key2=value2;\")\n";
  }

  protected String baseDescription(String jdbcType) {
    return String.format(
        "Read from a %s source using a SQL query or by directly accessing " + "a single table.\n",
        jdbcType);
  }

  protected String inheritedDescription(
      String prettyName, String transformName, String prefix, int port) {
    return String.format(
        "\n"
            + "This is a special case of ReadFromJdbc that includes the "
            + "necessary %s Driver and classes.\n"
            + "\n"
            + "An example of using %s with SQL query: ::\n"
            + "\n"
            + "    - type: %s\n"
            + "      config:\n"
            + "        url: \"jdbc:%s://my-host:%d/database\"\n"
            + "        query: \"SELECT * FROM table\"\n"
            + "\n"
            + "It is also possible to read a table by specifying a table name. For example, the "
            + "following configuration will perform a read on an entire table: ::\n"
            + "\n"
            + "    - type: %s\n"
            + "      config:\n"
            + "        url: \"jdbc:%s://my-host:%d/database\"\n"
            + "        table: \"my-table\"\n"
            + "\n"
            + "#### Advanced Usage\n"
            + "\n"
            + "It might be necessary to use a custom JDBC driver that is not packaged with this "
            + "transform. If that is the case, see ReadFromJdbc which "
            + "allows for more custom configuration.",
        prettyName, transformName, transformName, prefix, port, transformName, prefix, port);
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<JdbcReadSchemaTransformConfiguration>
      configurationClass() {
    return JdbcReadSchemaTransformConfiguration.class;
  }

  protected static void validateConfig(JdbcReadSchemaTransformConfiguration config, String jdbcType)
      throws IllegalArgumentException {
    if (Strings.isNullOrEmpty(config.getJdbcUrl())) {
      throw new IllegalArgumentException("JDBC URL cannot be blank");
    }

    boolean driverClassNamePresent = !Strings.isNullOrEmpty(config.getDriverClassName());
    boolean driverJarsPresent = !Strings.isNullOrEmpty(config.getDriverJars());
    boolean jdbcTypePresent = !Strings.isNullOrEmpty(jdbcType);
    if (!driverClassNamePresent && !driverJarsPresent && !jdbcTypePresent) {
      throw new IllegalArgumentException(
          "If JDBC type is not specified, then Driver Class Name and Driver Jars must be specified.");
    }
    if (!driverClassNamePresent && !jdbcTypePresent) {
      throw new IllegalArgumentException(
          "One of JDBC Driver class name or JDBC type must be specified.");
    }
    if (jdbcTypePresent
        && !JDBC_DRIVER_MAP.containsKey(Objects.requireNonNull(jdbcType).toLowerCase())) {
      throw new IllegalArgumentException("JDBC type must be one of " + JDBC_DRIVER_MAP.keySet());
    }

    boolean readQueryPresent = (config.getReadQuery() != null && !"".equals(config.getReadQuery()));
    boolean locationPresent = (config.getLocation() != null && !"".equals(config.getLocation()));

    if (readQueryPresent && locationPresent) {
      throw new IllegalArgumentException("Query and Table are mutually exclusive configurations");
    }
    if (!readQueryPresent && !locationPresent) {
      throw new IllegalArgumentException("Either Query or Table must be specified.");
    }
  }

  protected static void validateConfig(JdbcReadSchemaTransformConfiguration config)
      throws IllegalArgumentException {
    validateConfig(config, config.getJdbcType());
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcReadSchemaTransformConfiguration configuration) {
    validateConfig(configuration);
    return new JdbcReadSchemaTransform(configuration);
  }

  protected static class JdbcReadSchemaTransform extends SchemaTransform implements Serializable {

    JdbcReadSchemaTransformConfiguration config;
    private String jdbcType;

    public JdbcReadSchemaTransform(JdbcReadSchemaTransformConfiguration config) {
      this.config = config;
      this.jdbcType = config.getJdbcType();
    }

    public JdbcReadSchemaTransform(JdbcReadSchemaTransformConfiguration config, String jdbcType) {
      this.config = config;
      this.jdbcType = jdbcType;
    }

    protected JdbcIO.DataSourceConfiguration dataSourceConfiguration() {
      String driverClassName = config.getDriverClassName();

      if (Strings.isNullOrEmpty(driverClassName)) {
        driverClassName = JDBC_DRIVER_MAP.get(Objects.requireNonNull(jdbcType).toLowerCase());
      }

      JdbcIO.DataSourceConfiguration dsConfig =
          JdbcIO.DataSourceConfiguration.create(driverClassName, config.getJdbcUrl())
              .withUsername("".equals(config.getUsername()) ? null : config.getUsername())
              .withPassword("".equals(config.getPassword()) ? null : config.getPassword());
      String connectionProperties = config.getConnectionProperties();
      if (connectionProperties != null) {
        dsConfig = dsConfig.withConnectionProperties(connectionProperties);
      }

      List<@org.checkerframework.checker.nullness.qual.Nullable String> initialSql =
          config.getConnectionInitSql();
      if (initialSql != null && initialSql.size() > 0) {
        dsConfig = dsConfig.withConnectionInitSqls(initialSql);
      }

      String driverJars = config.getDriverJars();
      if (driverJars != null) {
        dsConfig = dsConfig.withDriverJars(config.getDriverJars());
      }

      return dsConfig;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      String query = config.getReadQuery();
      if (query == null) {
        query = String.format("SELECT * FROM %s", config.getLocation());
      }
      JdbcIO.ReadRows readRows =
          JdbcIO.readRows().withDataSourceConfiguration(dataSourceConfiguration()).withQuery(query);
      Integer fetchSize = config.getFetchSize();
      if (fetchSize != null && fetchSize > 0) {
        readRows = readRows.withFetchSize(fetchSize);
      }
      Boolean outputParallelization = config.getOutputParallelization();
      if (outputParallelization != null) {
        readRows = readRows.withOutputParallelization(outputParallelization);
      }
      Boolean disableAutoCommit = config.getDisableAutoCommit();
      if (disableAutoCommit != null) {
        readRows = readRows.withDisableAutoCommit(disableAutoCommit);
      }
      return PCollectionRowTuple.of("output", input.getPipeline().apply(readRows));
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class JdbcReadSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("Connection URL for the JDBC source.")
    public abstract String getJdbcUrl();

    @SchemaFieldDescription(
        "Sets the connection init sql statements used by the Driver. Only MySQL and MariaDB support this.")
    @Nullable
    public abstract List<@org.checkerframework.checker.nullness.qual.Nullable String>
        getConnectionInitSql();

    @SchemaFieldDescription(
        "Used to set connection properties passed to the JDBC driver not already defined as standalone parameter (e.g. username and password can be set using parameters above accordingly). Format of the string must be \"key1=value1;key2=value2;\".")
    @Nullable
    public abstract String getConnectionProperties();

    @SchemaFieldDescription(
        "Whether to disable auto commit on read. Defaults to true if not provided. The need for this config varies depending on the database platform. Informix requires this to be set to false while Postgres requires this to be set to true.")
    @Nullable
    public abstract Boolean getDisableAutoCommit();

    @SchemaFieldDescription(
        "Name of a Java Driver class to use to connect to the JDBC source. For example, \"com.mysql.jdbc.Driver\".")
    @Nullable
    public abstract String getDriverClassName();

    @SchemaFieldDescription(
        "Comma separated path(s) for the JDBC driver jar(s). This can be a local path or GCS (gs://) path.")
    @Nullable
    public abstract String getDriverJars();

    @SchemaFieldDescription(
        "This method is used to override the size of the data that is going to be fetched and loaded in memory per every database call. It should ONLY be used if the default value throws memory errors.")
    @Nullable
    public abstract Integer getFetchSize();

    @SchemaFieldDescription(
        "Type of JDBC source. When specified, an appropriate default Driver will be packaged with the transform. One of mysql, postgres, oracle, or mssql.")
    @Nullable
    public abstract String getJdbcType();

    @SchemaFieldDescription("Name of the table to read from.")
    @Nullable
    public abstract String getLocation();

    @SchemaFieldDescription(
        "Whether to reshuffle the resulting PCollection so results are distributed to all workers.")
    @Nullable
    public abstract Boolean getOutputParallelization();

    @SchemaFieldDescription("Password for the JDBC source.")
    @Nullable
    public abstract String getPassword();

    @SchemaFieldDescription("SQL query used to query the JDBC source.")
    @Nullable
    public abstract String getReadQuery();

    @SchemaFieldDescription("Username for the JDBC source.")
    @Nullable
    public abstract String getUsername();

    public static Builder builder() {
      return new AutoValue_JdbcReadSchemaTransformProvider_JdbcReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDriverClassName(String value);

      public abstract Builder setJdbcType(String value);

      public abstract Builder setJdbcUrl(String value);

      public abstract Builder setUsername(String value);

      public abstract Builder setPassword(String value);

      public abstract Builder setLocation(String value);

      public abstract Builder setReadQuery(String value);

      public abstract Builder setConnectionProperties(String value);

      public abstract Builder setConnectionInitSql(List<String> value);

      public abstract Builder setFetchSize(Integer value);

      public abstract Builder setOutputParallelization(Boolean value);

      public abstract Builder setDisableAutoCommit(Boolean value);

      public abstract Builder setDriverJars(String value);

      public abstract JdbcReadSchemaTransformConfiguration build();
    }
  }
}
