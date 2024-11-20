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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * An implementation of {@link org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider} for
 * writing to a JDBC connections using {@link org.apache.beam.sdk.io.jdbc.JdbcIO}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JdbcWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration> {

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:jdbc_write:v1";
  }

  @Override
  public String description() {
    return baseDescription("JDBC")
        + "\n"
        + "This transform can be used to write to a JDBC sink using either a given JDBC driver jar "
        + "and class name, or by using one of the default packaged drivers given a `jdbc_type`.\n"
        + "\n"
        + "#### Using a default driver\n"
        + "\n"
        + "This transform comes packaged with drivers for several popular JDBC distributions. The following "
        + "distributions can be declared as the `jdbc_type`: "
        + JDBC_DRIVER_MAP.keySet().toString().replaceAll("[\\[\\]]", "")
        + ".\n"
        + "\n"
        + "For example, writing to a MySQL sink using a SQL query: ::"
        + "\n"
        + "    - type: WriteToJdbc\n"
        + "      config:\n"
        + "        jdbc_type: mysql\n"
        + "        url: \"jdbc:mysql://my-host:3306/database\"\n"
        + "        query: \"INSERT INTO table VALUES(?, ?)\"\n"
        + "\n"
        + "\n"
        + "**Note**: See the following transforms which are built on top of this transform and simplify "
        + "this logic for several popular JDBC distributions:\n\n"
        + " - WriteToMySql\n"
        + " - WriteToPostgres\n"
        + " - WriteToOracle\n"
        + " - WriteToSqlServer\n"
        + "\n"
        + "#### Declaring custom JDBC drivers\n"
        + "\n"
        + "If writing to a JDBC sink not listed above, or if it is necessary to use a custom driver not "
        + "packaged with Beam, one must define a JDBC driver and class name.\n"
        + "\n"
        + "For example, writing to a MySQL table: ::"
        + "\n"
        + "    - type: WriteToJdbc\n"
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
        + "    - type: WriteToJdbc\n"
        + "      config:\n"
        + "        connectionProperties: \"characterEncoding=UTF-8;\"\n"
        + "        ...\n"
        + "All properties should be semi-colon-delimited (e.g. \"key1=value1;key2=value2;\")\n";
  }

  protected String baseDescription(String jdbcType) {
    return String.format(
        "Write to a %s sink using a SQL query or by directly accessing " + "a single table.\n",
        jdbcType);
  }

  protected String inheritedDescription(
      String prettyName, String transformName, String prefix, int port) {
    return String.format(
        "\n"
            + "This is a special case of WriteToJdbc that includes the "
            + "necessary %s Driver and classes.\n"
            + "\n"
            + "An example of using %s with SQL query: ::\n"
            + "\n"
            + "    - type: %s\n"
            + "      config:\n"
            + "        url: \"jdbc:%s://my-host:%d/database\"\n"
            + "        query: \"INSERT INTO table VALUES(?, ?)\"\n"
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
            + "transform. If that is the case, see WriteToJdbc which "
            + "allows for more custom configuration.",
        prettyName, transformName, transformName, prefix, port, transformName, prefix, port);
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<JdbcWriteSchemaTransformConfiguration>
      configurationClass() {
    return JdbcWriteSchemaTransformConfiguration.class;
  }

  protected String jdbcType() {
    return "";
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcWriteSchemaTransformConfiguration configuration) {
    configuration.validate(jdbcType());
    return new JdbcWriteSchemaTransform(configuration, jdbcType());
  }

  protected static class JdbcWriteSchemaTransform extends SchemaTransform implements Serializable {

    JdbcWriteSchemaTransformConfiguration config;
    private String jdbcType;

    public JdbcWriteSchemaTransform(JdbcWriteSchemaTransformConfiguration config, String jdbcType) {
      this.config = config;
      this.jdbcType = jdbcType;
    }

    protected JdbcIO.DataSourceConfiguration dataSourceConfiguration() {
      String driverClassName = config.getDriverClassName();

      if (Strings.isNullOrEmpty(driverClassName)) {
        driverClassName =
            JDBC_DRIVER_MAP.get(
                (Objects.requireNonNull(
                        !Strings.isNullOrEmpty(jdbcType) ? jdbcType : config.getJdbcType()))
                    .toLowerCase());
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

    protected String writeStatement(Schema schema) {
      String writeStatement = config.getWriteStatement();
      if (writeStatement != null) {
        return writeStatement;
      } else {
        StringBuilder statement = new StringBuilder("INSERT INTO ");
        statement.append(config.getLocation());
        statement.append(" (");
        statement.append(String.join(", ", schema.getFieldNames()));
        statement.append(") VALUES(");
        for (int i = 0; i < schema.getFieldCount() - 1; i++) {
          statement.append("?, ");
        }
        statement.append("?)");
        return statement.toString();
      }
    }

    private static class NoOutputDoFn<T> extends DoFn<T, Row> {
      @ProcessElement
      public void process(ProcessContext c) {}
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      JdbcIO.WriteVoid<Row> writeRows =
          JdbcIO.<Row>write()
              .withDataSourceConfiguration(dataSourceConfiguration())
              .withStatement(writeStatement(input.get("input").getSchema()))
              .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter())
              .withResults();
      Boolean autosharding = config.getAutosharding();
      if (autosharding != null && autosharding) {
        writeRows = writeRows.withAutoSharding();
      }

      Long writeBatchSize = config.getBatchSize();
      if (writeBatchSize != null) {
        writeRows = writeRows.withBatchSize(writeBatchSize);
      }

      PCollection<Row> postWrite =
          input
              .get("input")
              .apply(writeRows)
              .apply("post-write", ParDo.of(new NoOutputDoFn<>()))
              .setRowSchema(Schema.of());
      return PCollectionRowTuple.of("post_write", postWrite);
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.emptyList();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class JdbcWriteSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("Connection URL for the JDBC sink.")
    public abstract String getJdbcUrl();

    @SchemaFieldDescription(
        "If true, enables using a dynamically determined number of shards to write.")
    @Nullable
    public abstract Boolean getAutosharding();

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
        "Name of a Java Driver class to use to connect to the JDBC source. For example, \"com.mysql.jdbc.Driver\".")
    @Nullable
    public abstract String getDriverClassName();

    @SchemaFieldDescription(
        "Comma separated path(s) for the JDBC driver jar(s). This can be a local path or GCS (gs://) path.")
    @Nullable
    public abstract String getDriverJars();

    @Nullable
    public abstract Long getBatchSize();

    public void validate() throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getJdbcUrl())) {
        throw new IllegalArgumentException("JDBC URL cannot be blank");
      }
    @SchemaFieldDescription(
        "Type of JDBC source. When specified, an appropriate default Driver will be packaged with the transform. One of mysql, postgres, oracle, or mssql.")
    @Nullable
    public abstract String getJdbcType();

    @SchemaFieldDescription("Name of the table to write to.")
    @Nullable
    public abstract String getLocation();

    @SchemaFieldDescription("Password for the JDBC source.")
    @Nullable
    public abstract String getPassword();

    @SchemaFieldDescription("Username for the JDBC source.")
    @Nullable
    public abstract String getUsername();

    @SchemaFieldDescription("SQL query used to insert records into the JDBC sink.")
    @Nullable
    public abstract String getWriteStatement();

    public void validate() {
      validate("JDBC");
    }

    public void validate(String jdbcType) throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getJdbcUrl())) {
        throw new IllegalArgumentException("JDBC URL cannot be blank");
      }

      jdbcType = !Strings.isNullOrEmpty(jdbcType) ? jdbcType : getJdbcType();

      boolean driverClassNamePresent = !Strings.isNullOrEmpty(getDriverClassName());
      boolean driverJarsPresent = !Strings.isNullOrEmpty(getDriverJars());
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

      boolean writeStatementPresent =
          (getWriteStatement() != null && !"".equals(getWriteStatement()));
      boolean locationPresent = (getLocation() != null && !"".equals(getLocation()));

      if (writeStatementPresent && locationPresent) {
        throw new IllegalArgumentException(
            "Write Statement and Table are mutually exclusive configurations");
      }
      if (!writeStatementPresent && !locationPresent) {
        throw new IllegalArgumentException("Either Write Statement or Table must be set.");
      }
    }

    public static Builder builder() {
      return new AutoValue_JdbcWriteSchemaTransformProvider_JdbcWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDriverClassName(String value);

      public abstract Builder setJdbcType(String value);

      public abstract Builder setJdbcUrl(String value);

      public abstract Builder setUsername(String value);

      public abstract Builder setPassword(String value);

      public abstract Builder setConnectionProperties(String value);

      public abstract Builder setConnectionInitSql(
          List<@org.checkerframework.checker.nullness.qual.Nullable String> value);

      public abstract Builder setLocation(String value);

      public abstract Builder setWriteStatement(String value);

      public abstract Builder setAutosharding(Boolean value);

      public abstract Builder setDriverJars(String value);

      public abstract Builder setBatchSize(Long value);

      public abstract JdbcWriteSchemaTransformConfiguration build();
    }
  }
}
