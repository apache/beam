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

import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.toRow;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/** A {@link SchemaTransformProvider} for reading rows from Snowflake. */
@AutoService(SchemaTransformProvider.class)
public class SnowflakeReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<SnowflakeReadSchemaTransformProvider.Configuration> {

  static final String OUTPUT_TAG = "output";

  public static final String IDENTIFIER = "beam:schematransform:org.apache.beam:snowflake_read:v1";

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public String description() {
    return "Reads rows from a Snowflake table or query using staged CSV files.";
  }

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    configuration.validate();
    return new SnowflakeReadSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  private static class SnowflakeReadSchemaTransform extends SchemaTransform
      implements Serializable {

    private final Configuration configuration;

    private SnowflakeReadSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema outputSchema = JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema());

      SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
          SnowflakeSchemaTransformUtils.createDataSourceConfiguration(
              configuration.getServerName(),
              configuration.getUsername(),
              configuration.getPassword(),
              configuration.getOauthToken(),
              configuration.getPrivateKey(),
              configuration.getPrivateKeyPassphrase(),
              configuration.getDatabase(),
              configuration.getSnowflakeSchema(),
              configuration.getWarehouse(),
              configuration.getRole());

      SnowflakeIO.Read<Row> read =
          SnowflakeIO.<Row>read()
              .withDataSourceConfiguration(dataSourceConfiguration)
              .withStagingBucketName(configuration.getStagingBucketName())
              .withStorageIntegrationName(configuration.getStorageIntegrationName())
              .withCsvMapper(parts -> toRow(parts, outputSchema))
              .withCoder(RowCoder.of(outputSchema));

      String table = configuration.getTable();
      if (table != null) {
        read = read.fromTable(table);
      } else {
        String query = configuration.getQuery();
        if (query != null) {
          read = read.fromQuery(query);
        }
      }

      String quotationMark = configuration.getQuotationMark();
      if (quotationMark != null) {
        read = read.withQuotationMark(quotationMark);
      }

      PCollection<Row> rows =
          input.getPipeline().apply("ReadFromSnowflake", read).setRowSchema(outputSchema);

      return PCollectionRowTuple.of(OUTPUT_TAG, rows);
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Configuration implements Serializable {

    @SchemaFieldDescription("Snowflake server name.")
    public abstract String getServerName();

    @SchemaFieldDescription(
        "Snowflake username. Required for password and private key authentication.")
    @Nullable
    public abstract String getUsername();

    @SchemaFieldDescription(
        "Snowflake password. Mutually exclusive with OAuth token and private key.")
    @Nullable
    public abstract String getPassword();

    @SchemaFieldDescription(
        "Snowflake OAuth token. Mutually exclusive with password and private key.")
    @Nullable
    public abstract String getOauthToken();

    @SchemaFieldDescription(
        "Raw Snowflake private key. Mutually exclusive with password and OAuth token.")
    @Nullable
    public abstract String getPrivateKey();

    @SchemaFieldDescription("Passphrase for the Snowflake private key.")
    @Nullable
    public abstract String getPrivateKeyPassphrase();

    @SchemaFieldDescription("Snowflake database name.")
    public abstract String getDatabase();

    @SchemaFieldDescription("Snowflake schema name.")
    public abstract String getSnowflakeSchema();

    @SchemaFieldDescription("Snowflake warehouse name.")
    @Nullable
    public abstract String getWarehouse();

    @SchemaFieldDescription("Snowflake role.")
    @Nullable
    public abstract String getRole();

    @SchemaFieldDescription("Snowflake table to read from.")
    @Nullable
    public abstract String getTable();

    @SchemaFieldDescription("Snowflake query to read from.")
    @Nullable
    public abstract String getQuery();

    @SchemaFieldDescription("GCS path used to stage CSV files. The path must end with '/'.")
    public abstract String getStagingBucketName();

    @SchemaFieldDescription("Snowflake storage integration name.")
    public abstract String getStorageIntegrationName();

    @SchemaFieldDescription("Output schema encoded using JSON Schema syntax.")
    public abstract String getSchema();

    @SchemaFieldDescription("Quotation mark used when parsing staged CSV files.")
    @Nullable
    public abstract String getQuotationMark();

    public static Builder builder() {
      return new AutoValue_SnowflakeReadSchemaTransformProvider_Configuration.Builder();
    }

    public abstract Builder toBuilder();

    void validate() {
      requireNonEmpty(getServerName(), "serverName");
      requireNonEmpty(getDatabase(), "database");
      requireNonEmpty(getSnowflakeSchema(), "snowflakeSchema");
      requireNonEmpty(getStagingBucketName(), "stagingBucketName");
      requireNonEmpty(getStorageIntegrationName(), "storageIntegrationName");
      requireNonEmpty(getSchema(), "schema");

      SnowflakeSchemaTransformUtils.validateAuthentication(
          getUsername(),
          getPassword(),
          getOauthToken(),
          getPrivateKey(),
          getPrivateKeyPassphrase());

      String table = getTable();
      boolean tablePresent = table != null && !table.isEmpty();
      String query = getQuery();
      boolean queryPresent = query != null && !query.isEmpty();

      if (!tablePresent && !queryPresent) {
        throw new IllegalArgumentException("Either table or query must be specified.");
      }

      if (tablePresent && queryPresent) {
        throw new IllegalArgumentException("table and query are mutually exclusive.");
      }

      if (!getStagingBucketName().endsWith("/")) {
        throw new IllegalArgumentException("stagingBucketName must end with '/'");
      }

      // Validate JSON schema early.
      JsonUtils.beamSchemaFromJsonSchema(getSchema());
    }

    private static void requireNonEmpty(String value, String name) {
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException(name + " cannot be empty");
      }
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setServerName(String value);

      public abstract Builder setUsername(@Nullable String value);

      public abstract Builder setPassword(@Nullable String value);

      public abstract Builder setOauthToken(@Nullable String value);

      public abstract Builder setPrivateKey(@Nullable String value);

      public abstract Builder setPrivateKeyPassphrase(@Nullable String value);

      public abstract Builder setDatabase(String value);

      public abstract Builder setSnowflakeSchema(String value);

      public abstract Builder setWarehouse(@Nullable String value);

      public abstract Builder setRole(@Nullable String value);

      public abstract Builder setTable(@Nullable String value);

      public abstract Builder setQuery(@Nullable String value);

      public abstract Builder setStagingBucketName(String value);

      public abstract Builder setStorageIntegrationName(String value);

      public abstract Builder setSchema(String value);

      public abstract Builder setQuotationMark(@Nullable String value);

      public abstract Configuration build();
    }
  }
}
