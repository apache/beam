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
  protected @UnknownKeyFor @NonNull @Initialized Class<JdbcWriteSchemaTransformConfiguration>
      configurationClass() {
    return JdbcWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcWriteSchemaTransformConfiguration configuration) {
    configuration.validate();
    return new JdbcWriteSchemaTransform(configuration);
  }

  static class JdbcWriteSchemaTransform extends SchemaTransform implements Serializable {

    JdbcWriteSchemaTransformConfiguration config;

    public JdbcWriteSchemaTransform(JdbcWriteSchemaTransformConfiguration config) {
      this.config = config;
    }

    protected JdbcIO.DataSourceConfiguration dataSourceConfiguration() {
      String driverClassName = config.getDriverClassName();

      if (Strings.isNullOrEmpty(driverClassName)) {
        driverClassName =
            JDBC_DRIVER_MAP.get(Objects.requireNonNull(config.getJdbcType()).toLowerCase());
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
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:jdbc_write:v1";
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

    @Nullable
    public abstract String getDriverClassName();

    @Nullable
    public abstract String getJdbcType();

    public abstract String getJdbcUrl();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract String getConnectionProperties();

    @Nullable
    public abstract List<@org.checkerframework.checker.nullness.qual.Nullable String>
        getConnectionInitSql();

    @Nullable
    public abstract String getLocation();

    @Nullable
    public abstract String getWriteStatement();

    @Nullable
    public abstract Boolean getAutosharding();

    @Nullable
    public abstract String getDriverJars();

    public void validate() throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getJdbcUrl())) {
        throw new IllegalArgumentException("JDBC URL cannot be blank");
      }

      boolean driverClassNamePresent = !Strings.isNullOrEmpty(getDriverClassName());
      boolean jdbcTypePresent = !Strings.isNullOrEmpty(getJdbcType());
      if (driverClassNamePresent && jdbcTypePresent) {
        throw new IllegalArgumentException(
            "JDBC Driver class name and JDBC type are mutually exclusive configurations.");
      }
      if (!driverClassNamePresent && !jdbcTypePresent) {
        throw new IllegalArgumentException(
            "One of JDBC Driver class name or JDBC type must be specified.");
      }
      if (jdbcTypePresent
          && !JDBC_DRIVER_MAP.containsKey(Objects.requireNonNull(getJdbcType()).toLowerCase())) {
        throw new IllegalArgumentException("JDBC type must be one of " + JDBC_DRIVER_MAP.keySet());
      }

      boolean writeStatementPresent =
          (getWriteStatement() != null && !"".equals(getWriteStatement()));
      boolean locationPresent = (getLocation() != null && !"".equals(getLocation()));

      if (writeStatementPresent && locationPresent) {
        throw new IllegalArgumentException(
            "ReadQuery and Location are mutually exclusive configurations");
      }
      if (!writeStatementPresent && !locationPresent) {
        throw new IllegalArgumentException("Either ReadQuery or Location must be set.");
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

      public abstract JdbcWriteSchemaTransformConfiguration build();
    }
  }
}
