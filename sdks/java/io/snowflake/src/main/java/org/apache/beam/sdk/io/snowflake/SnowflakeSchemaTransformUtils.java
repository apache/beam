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
import org.apache.beam.sdk.io.snowflake.enums.StreamingLogLevel;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.joda.time.Instant;

/** Utilities shared by Snowflake schema transform providers. */
public class SnowflakeSchemaTransformUtils {

  public static SnowflakeIO.DataSourceConfiguration createDataSourceConfiguration(
      String serverName,
      @Nullable String username,
      @Nullable String password,
      @Nullable String oauthToken,
      @Nullable String privateKey,
      @Nullable String privateKeyPassphrase,
      String database,
      String snowflakeSchema,
      @Nullable String warehouse,
      @Nullable String role) {

    SnowflakeIO.DataSourceConfiguration configuration =
        SnowflakeIO.DataSourceConfiguration.create();

    if (isNotEmpty(password) && isNotEmpty(username)) {
      configuration = configuration.withUsernamePasswordAuth(username, password);
    } else if (isNotEmpty(oauthToken)) {
      configuration = configuration.withOAuth(oauthToken);
    } else if (isNotEmpty(privateKey) && isNotEmpty(username)) {
      if (isNotEmpty(privateKeyPassphrase)) {
        configuration =
            configuration.withKeyPairRawAuth(username, privateKey, privateKeyPassphrase);
      } else {
        configuration = configuration.withKeyPairRawAuth(username, privateKey);
      }
    }

    configuration =
        configuration.withServerName(serverName).withDatabase(database).withSchema(snowflakeSchema);

    if (isNotEmpty(warehouse)) {
      configuration = configuration.withWarehouse(warehouse);
    }

    if (isNotEmpty(role)) {
      configuration = configuration.withRole(role);
    }

    return configuration;
  }

  public static void validateAuthentication(
      @Nullable String username,
      @Nullable String password,
      @Nullable String oauthToken,
      @Nullable String privateKey,
      @Nullable String privateKeyPassphrase) {

    int authenticationMethods = 0;

    if (isNotEmpty(password)) {
      authenticationMethods++;
    }

    if (isNotEmpty(oauthToken)) {
      authenticationMethods++;
    }

    if (isNotEmpty(privateKey)) {
      authenticationMethods++;
    }

    if (authenticationMethods != 1) {
      throw new IllegalArgumentException(
          "Exactly one authentication method must be configured: "
              + "password, oauthToken, or privateKey.");
    }

    if ((isNotEmpty(password) || isNotEmpty(privateKey)) && !isNotEmpty(username)) {
      throw new IllegalArgumentException(
          "username is required for password and private key authentication.");
    }

    if (isNotEmpty(privateKeyPassphrase) && !isNotEmpty(privateKey)) {
      throw new IllegalArgumentException("privateKeyPassphrase requires privateKey.");
    }
  }

  @EnsuresNonNullIf(expression = "#1", result = true)
  public static boolean isNotEmpty(@Nullable String value) {
    return value != null && !value.isEmpty();
  }

  public static StreamingLogLevel parseStreamingLogLevel(String value) {
    try {
      return StreamingLogLevel.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported debugMode '" + value + "'. Supported values are ERROR and INFO.", e);
    }
  }

  public static CreateDisposition parseCreateDisposition(String value) {
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

  public static WriteDisposition parseWriteDisposition(String value) {
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

  public static SnowflakeTableSchema toSnowflakeTableSchema(Schema schema) {
    SnowflakeColumn[] columns =
        schema.getFields().stream()
            .map(SnowflakeSchemaTransformUtils::toSnowflakeColumn)
            .toArray(SnowflakeColumn[]::new);

    return SnowflakeTableSchema.of(columns);
  }

  public static SnowflakeColumn toSnowflakeColumn(Schema.Field field) {
    SnowflakeDataType snowflakeType = toSnowflakeDataType(field);

    return SnowflakeColumn.of(field.getName(), snowflakeType, field.getType().getNullable());
  }

  public static Row toRow(String[] parts, Schema schema) {
    if (parts.length != schema.getFieldCount()) {
      throw new IllegalArgumentException(
          String.format(
              "Snowflake row contains %d values, but the configured schema contains %d fields.",
              parts.length, schema.getFieldCount()));
    }

    Row.Builder builder = Row.withSchema(schema);

    for (int i = 0; i < schema.getFieldCount(); i++) {
      Schema.Field field = schema.getField(i);
      builder.addValue(toBeamValue(parts[i], field));
    }

    return builder.build();
  }

  public static @Nullable Object toBeamValue(String value, Schema.Field field) {
    if (value == null || value.isEmpty()) {
      if (field.getType().getNullable()) {
        return null;
      }

      /*
       * Snowflake COPY encodes NULL as an empty CSV value. Therefore an empty
       * value cannot be represented for a required non-string type.
       *
       * For STRING, preserve the empty string.
       */
      if (field.getType().getTypeName() == Schema.TypeName.STRING) {
        return "";
      }

      throw new IllegalArgumentException(
          String.format(
              "Received an empty value for non-nullable Snowflake field '%s'.", field.getName()));
    }

    try {
      switch (field.getType().getTypeName()) {
        case BYTE:
          return Byte.valueOf(value);

        case INT16:
          return Short.valueOf(value);

        case INT32:
          return Integer.valueOf(value);

        case INT64:
          return Long.valueOf(value);

        case FLOAT:
          return Float.valueOf(value);

        case DOUBLE:
          return Double.valueOf(value);

        case STRING:
          return value;

        case BOOLEAN:
          if ("true".equalsIgnoreCase(value)) {
            return true;
          }

          if ("false".equalsIgnoreCase(value)) {
            return false;
          }

          throw new IllegalArgumentException(String.format("Invalid boolean value '%s'.", value));

        case BYTES:
          return decodeHex(value);

        case DATETIME:
          return Instant.parse(value);

        case DECIMAL:
        case ARRAY:
        case ITERABLE:
        case MAP:
        case ROW:
        case LOGICAL_TYPE:
        default:
          throw unsupportedFieldType(field, null);
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to parse value '%s' as %s for Snowflake field '%s'.",
              value, field.getType().getTypeName(), field.getName()),
          e);
    }
  }

  private static byte[] decodeHex(String value) {
    if ((value.length() & 1) != 0) {
      throw new IllegalArgumentException("Invalid hexadecimal Snowflake binary value.");
    }

    byte[] result = new byte[value.length() / 2];

    for (int i = 0; i < value.length(); i += 2) {
      int high = Character.digit(value.charAt(i), 16);
      int low = Character.digit(value.charAt(i + 1), 16);

      if (high == -1 || low == -1) {
        throw new IllegalArgumentException("Invalid hexadecimal Snowflake binary value.");
      }

      result[i / 2] = (byte) ((high << 4) | low);
    }

    return result;
  }

  public static SnowflakeDataType toSnowflakeDataType(Schema.Field field) {
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

  public static IllegalArgumentException unsupportedFieldType(
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
