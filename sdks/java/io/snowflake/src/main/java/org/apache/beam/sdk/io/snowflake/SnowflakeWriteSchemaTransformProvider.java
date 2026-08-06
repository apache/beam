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
package org.apache.beam.sdk.io.snowflake;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeDataType;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/** A {@link SchemaTransformProvider} for writing Beam rows to Snowflake. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class SnowflakeWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<SnowflakeWriteSchemaTransformProvider.Configuration> {

  static final String INPUT_TAG = "input";

  public static final String IDENTIFIER = "beam:schematransform:org.apache.beam:snowflake_write:v1";

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public String description() {
    return "Writes Beam Rows to a Snowflake table using staged CSV files.";
  }

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    configuration.validate();
    return new SnowflakeWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  /** Schema transform that configures and applies {@link SnowflakeIO.Write}. */
  private static class SnowflakeWriteSchemaTransform extends SchemaTransform
      implements Serializable {

    private final Configuration configuration;

    private SnowflakeWriteSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);

      SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
          SnowflakeIO.DataSourceConfiguration.create()
              .withUsernamePasswordAuth(configuration.getUsername(), configuration.getPassword())
              .withServerName(configuration.getServerName())
              .withDatabase(configuration.getDatabase())
              .withSchema(configuration.getSchema());

      if (configuration.getWarehouse() != null) {
        dataSourceConfiguration =
            dataSourceConfiguration.withWarehouse(configuration.getWarehouse());
      }

      if (configuration.getRole() != null) {
        dataSourceConfiguration = dataSourceConfiguration.withRole(configuration.getRole());
      }

      SnowflakeIO.Write<Row> write =
          SnowflakeIO.<Row>write()
              .withDataSourceConfiguration(dataSourceConfiguration)
              .withStagingBucketName(configuration.getStagingBucketName())
              .withStorageIntegrationName(configuration.getStorageIntegrationName())
              .withUserDataMapper(row -> row.getValues().toArray())
              .to(configuration.getTable());

      if (configuration.getCreateDisposition() != null) {
        CreateDisposition createDisposition =
            parseCreateDisposition(configuration.getCreateDisposition());

        write = write.withCreateDisposition(createDisposition);

        if (createDisposition == CreateDisposition.CREATE_IF_NEEDED) {
          write = write.withTableSchema(toSnowflakeTableSchema(rows.getSchema()));
        }
      }

      if (configuration.getWriteDisposition() != null) {
        write =
            write.withWriteDisposition(parseWriteDisposition(configuration.getWriteDisposition()));
      }

      if (configuration.getQuotationMark() != null) {
        write = write.withQuotationMark(configuration.getQuotationMark());
      }

      rows.apply("WriteToSnowflake", write);

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {

    @SchemaFieldDescription("Snowflake server name.")
    public abstract String getServerName();

    @SchemaFieldDescription("Snowflake username.")
    public abstract String getUsername();

    @SchemaFieldDescription("Snowflake password.")
    public abstract String getPassword();

    @SchemaFieldDescription("Snowflake database name.")
    public abstract String getDatabase();

    @SchemaFieldDescription("Snowflake schema name.")
    public abstract String getSchema();

    @SchemaFieldDescription("Snowflake warehouse name.")
    @Nullable
    public abstract String getWarehouse();

    @SchemaFieldDescription("Snowflake role.")
    @Nullable
    public abstract String getRole();

    @SchemaFieldDescription("Destination Snowflake table.")
    public abstract String getTable();

    @SchemaFieldDescription("GCS path used to stage CSV files. The path must end with '/'.")
    public abstract String getStagingBucketName();

    @SchemaFieldDescription("Snowflake storage integration name.")
    public abstract String getStorageIntegrationName();

    @SchemaFieldDescription(
        "Table creation behavior. Supported values are CREATE_IF_NEEDED and CREATE_NEVER.")
    @Nullable
    public abstract String getCreateDisposition();

    @SchemaFieldDescription("Write behavior. Supported values are APPEND, TRUNCATE, and EMPTY.")
    @Nullable
    public abstract String getWriteDisposition();

    @SchemaFieldDescription("Quotation mark used when writing values to staged CSV files.")
    @Nullable
    public abstract String getQuotationMark();

    public static Builder builder() {
      return new AutoValue_SnowflakeWriteSchemaTransformProvider_Configuration.Builder();
    }

    public abstract Builder toBuilder();

    void validate() {
      requireNonEmpty(getServerName(), "serverName");
      requireNonEmpty(getUsername(), "username");
      requireNonEmpty(getPassword(), "password");
      requireNonEmpty(getDatabase(), "database");
      requireNonEmpty(getSchema(), "schema");
      requireNonEmpty(getTable(), "table");
      requireNonEmpty(getStagingBucketName(), "stagingBucketName");
      requireNonEmpty(getStorageIntegrationName(), "storageIntegrationName");

      if (!getStagingBucketName().endsWith("/")) {
        throw new IllegalArgumentException("stagingBucketName must end with '/'");
      }

      if (getCreateDisposition() != null) {
        parseCreateDisposition(getCreateDisposition());
      }

      if (getWriteDisposition() != null) {
        parseWriteDisposition(getWriteDisposition());
      }
    }

    private static void requireNonEmpty(String value, String name) {
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException(name + " cannot be empty");
      }
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setServerName(String value);

      public abstract Builder setUsername(String value);

      public abstract Builder setPassword(String value);

      public abstract Builder setDatabase(String value);

      public abstract Builder setSchema(String value);

      public abstract Builder setWarehouse(String value);

      public abstract Builder setRole(String value);

      public abstract Builder setTable(String value);

      public abstract Builder setStagingBucketName(String value);

      public abstract Builder setStorageIntegrationName(String value);

      public abstract Builder setCreateDisposition(String value);

      public abstract Builder setWriteDisposition(String value);

      public abstract Builder setQuotationMark(String value);

      public abstract Configuration build();
    }
  }

  private static CreateDisposition parseCreateDisposition(String value) {
    try {
      return CreateDisposition.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported createDisposition '"
              + value
              + "'. Supported values are CREATE_IF_NEEDED and CREATE_NEVER.",
          e);
    }
  }

  private static WriteDisposition parseWriteDisposition(String value) {
    try {
      return WriteDisposition.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported writeDisposition '"
              + value
              + "'. Supported values are APPEND, TRUNCATE, and EMPTY.",
          e);
    }
  }

  static SnowflakeTableSchema toSnowflakeTableSchema(Schema schema) {
    SnowflakeColumn[] columns =
        schema.getFields().stream()
            .map(SnowflakeWriteSchemaTransformProvider::toSnowflakeColumn)
            .toArray(SnowflakeColumn[]::new);

    return SnowflakeTableSchema.of(columns);
  }

  private static SnowflakeColumn toSnowflakeColumn(Schema.Field field) {
    SnowflakeDataType snowflakeType = toSnowflakeDataType(field);

    return SnowflakeColumn.of(field.getName(), snowflakeType, field.getType().getNullable());
  }

  private static SnowflakeDataType toSnowflakeDataType(Schema.Field field) {
    switch (field.getType().getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
        return SnowflakeNumber.of();

      case FLOAT:
      case DOUBLE:
        return SnowflakeDouble.of();

      case STRING:
        return SnowflakeVarchar.of();

      case BOOLEAN:
        return SnowflakeBoolean.of();

      case BYTES:
        return SnowflakeBinary.of();

      case DATETIME:
        return SnowflakeTimestamp.of();

      case DECIMAL:
        throw unsupportedFieldType(
            field, "Beam DECIMAL does not include Snowflake precision and scale information.");

      case ARRAY:
      case ITERABLE:
      case MAP:
      case ROW:
      case LOGICAL_TYPE:
      default:
        throw unsupportedFieldType(field, null);
    }
  }

  private static IllegalArgumentException unsupportedFieldType(
      Schema.Field field, @Nullable String details) {
    String message =
        String.format(
            "Unsupported Beam field type %s for Snowflake column '%s'.",
            field.getType().getTypeName(), field.getName());

    if (details != null) {
      message += " " + details;
    }

    return new IllegalArgumentException(message);
  }
}
