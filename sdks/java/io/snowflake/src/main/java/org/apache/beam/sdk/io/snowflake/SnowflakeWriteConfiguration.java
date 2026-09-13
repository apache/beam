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

import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseCreateDisposition;
import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseStreamingLogLevel;
import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseWriteDisposition;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class SnowflakeWriteConfiguration implements Serializable {

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
  public abstract String getSchema();

  @SchemaFieldDescription("Snowflake warehouse name.")
  @Nullable
  public abstract String getWarehouse();

  @SchemaFieldDescription("Snowflake role.")
  @Nullable
  public abstract String getRole();

  @SchemaFieldDescription("Destination Snowflake table. Required for batch writes.")
  @Nullable
  public abstract String getTable();

  @SchemaFieldDescription("Snowflake Snowpipe name. Required for streaming writes.")
  @Nullable
  public abstract String getSnowPipe();

  @SchemaFieldDescription("GCS path used to stage CSV files. The path must end with '/'.")
  public abstract String getStagingBucketName();

  @SchemaFieldDescription("Snowflake storage integration name.")
  public abstract String getStorageIntegrationName();

  @SchemaFieldDescription(
      "Table creation behavior for batch writes. "
          + "Supported values are CREATE_IF_NEEDED and CREATE_NEVER.")
  @Nullable
  public abstract String getCreateDisposition();

  @SchemaFieldDescription(
      "Write behavior for batch writes. " + "Supported values are APPEND, TRUNCATE, and EMPTY.")
  @Nullable
  public abstract String getWriteDisposition();

  @SchemaFieldDescription("Quotation mark used when writing values to staged CSV files.")
  @Nullable
  public abstract String getQuotationMark();

  @SchemaFieldDescription("Maximum number of rows to stage before flushing in streaming mode.")
  @Nullable
  public abstract Integer getFlushRowLimit();

  @SchemaFieldDescription(
      "Maximum time in milliseconds before flushing staged rows in streaming mode.")
  @Nullable
  public abstract Long getFlushTimeLimitMillis();

  @SchemaFieldDescription("Number of output shards used when staging files.")
  @Nullable
  public abstract Integer getShardsNumber();

  @SchemaFieldDescription("Streaming log level. Supported values are ERROR and INFO.")
  @Nullable
  public abstract String getDebugMode();

  public static Builder builder() {
    return new AutoValue_SnowflakeWriteConfiguration.Builder();
  }

  public abstract Builder toBuilder();

  void validate() {
    requireNonEmpty(getServerName(), "serverName");
    requireNonEmpty(getDatabase(), "database");
    requireNonEmpty(getSchema(), "schema");
    requireNonEmpty(getStagingBucketName(), "stagingBucketName");
    requireNonEmpty(getStorageIntegrationName(), "storageIntegrationName");

    SnowflakeSchemaTransformUtils.validateAuthentication(
        getUsername(), getPassword(), getOauthToken(), getPrivateKey(), getPrivateKeyPassphrase());

    if (!getStagingBucketName().endsWith("/")) {
      throw new IllegalArgumentException("stagingBucketName must end with '/'");
    }

    // Parse configured enum values to validate that they are supported.
    String createDisposition = getCreateDisposition();
    if (createDisposition != null) {
      parseCreateDisposition(createDisposition);
    }

    String writeDisposition = getWriteDisposition();
    if (writeDisposition != null) {
      parseWriteDisposition(writeDisposition);
    }

    String debugMode = getDebugMode();
    if (debugMode != null) {
      parseStreamingLogLevel(debugMode);
    }

    Integer flushRowLimit = getFlushRowLimit();
    if (flushRowLimit != null && flushRowLimit <= 0) {
      throw new IllegalArgumentException("flushRowLimit must be greater than 0.");
    }

    Long flushTimeLimitMillis = getFlushTimeLimitMillis();
    if (flushTimeLimitMillis != null && flushTimeLimitMillis <= 0) {
      throw new IllegalArgumentException("flushTimeLimitMillis must be greater than 0.");
    }

    Integer shardsNumber = getShardsNumber();
    if (shardsNumber != null && shardsNumber <= 0) {
      throw new IllegalArgumentException("shardsNumber must be greater than 0.");
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

    public abstract Builder setUsername(@Nullable String value);

    public abstract Builder setPassword(@Nullable String value);

    public abstract Builder setOauthToken(@Nullable String value);

    public abstract Builder setPrivateKey(@Nullable String value);

    public abstract Builder setPrivateKeyPassphrase(@Nullable String value);

    public abstract Builder setDatabase(String value);

    public abstract Builder setSchema(String value);

    public abstract Builder setWarehouse(@Nullable String value);

    public abstract Builder setRole(@Nullable String value);

    public abstract Builder setTable(@Nullable String value);

    public abstract Builder setSnowPipe(@Nullable String value);

    public abstract Builder setStagingBucketName(String value);

    public abstract Builder setStorageIntegrationName(String value);

    public abstract Builder setCreateDisposition(@Nullable String value);

    public abstract Builder setWriteDisposition(@Nullable String value);

    public abstract Builder setQuotationMark(@Nullable String value);

    public abstract Builder setFlushRowLimit(@Nullable Integer value);

    public abstract Builder setFlushTimeLimitMillis(@Nullable Long value);

    public abstract Builder setShardsNumber(@Nullable Integer value);

    public abstract Builder setDebugMode(@Nullable String value);

    public abstract SnowflakeWriteConfiguration build();
  }
}
