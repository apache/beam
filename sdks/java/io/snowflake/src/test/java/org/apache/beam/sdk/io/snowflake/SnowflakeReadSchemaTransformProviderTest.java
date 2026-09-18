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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.snowflake.SnowflakeReadSchemaTransformProvider.Configuration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeReadSchemaTransformProviderTest {

  private static final String SIMPLE_SCHEMA =
      "{"
          + "\"type\":\"object\","
          + "\"properties\":{"
          + "\"id\":{\"type\":\"integer\"},"
          + "\"name\":{\"type\":\"string\"}"
          + "},"
          + "\"required\":[\"id\",\"name\"]"
          + "}";

  private final SnowflakeReadSchemaTransformProvider provider =
      new SnowflakeReadSchemaTransformProvider();

  @Test
  public void testIdentifier() {
    assertThat(
        provider.identifier(), equalTo("beam:schematransform:org.apache.beam:snowflake_read:v1"));
  }

  @Test
  public void testInputCollectionNames() {
    assertThat(provider.inputCollectionNames(), empty());
  }

  @Test
  public void testOutputCollectionNames() {
    assertThat(provider.outputCollectionNames(), contains("output"));
  }

  @Test
  public void testValidTableConfiguration() {
    provider.from(validConfiguration().setTable("table").build());
  }

  @Test
  public void testValidQueryConfiguration() {
    provider.from(validConfiguration().setQuery("SELECT * FROM table").build());
  }

  @Test
  public void testTableAndQueryAreMutuallyExclusive() {
    Configuration configuration =
        validConfiguration().setTable("table").setQuery("SELECT * FROM table").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("table and query are mutually exclusive."));
  }

  @Test
  public void testTableOrQueryIsRequired() {
    Configuration configuration = validConfiguration().build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("Either table or query must be specified."));
  }

  @Test
  public void testStagingBucketMustEndWithSlash() {
    Configuration configuration =
        validConfiguration().setTable("table").setStagingBucketName("gs://bucket/staging").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("stagingBucketName must end with '/'"));
  }

  @Test
  public void testConvertsCsvValuesToBeamRow() {
    Schema schema =
        Schema.builder()
            .addByteField("byte_value")
            .addInt16Field("short_value")
            .addInt32Field("int_value")
            .addInt64Field("long_value")
            .addFloatField("float_value")
            .addDoubleField("double_value")
            .addStringField("string_value")
            .addBooleanField("boolean_value")
            .addByteArrayField("bytes_value")
            .addDateTimeField("datetime_value")
            .build();

    String[] values = {
      "1", "2", "3", "4", "5.5", "6.5", "hello", "true", "616263", "2026-08-13T09:00:00.000Z"
    };

    Row row = SnowflakeSchemaTransformUtils.toRow(values, schema);

    assertThat(row.getByte("byte_value"), equalTo((byte) 1));
    assertThat(row.getInt16("short_value"), equalTo((short) 2));
    assertThat(row.getInt32("int_value"), equalTo(3));
    assertThat(row.getInt64("long_value"), equalTo(4L));
    assertThat(row.getFloat("float_value"), equalTo(5.5F));
    assertThat(row.getDouble("double_value"), equalTo(6.5D));
    assertThat(row.getString("string_value"), equalTo("hello"));
    assertThat(row.getBoolean("boolean_value"), equalTo(true));
    assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), row.getBytes("bytes_value"));
    assertThat(
        row.getDateTime("datetime_value"), equalTo(Instant.parse("2026-08-13T09:00:00.000Z")));
  }

  @Test
  public void testNullableEmptyValueBecomesNull() {
    Schema schema = Schema.builder().addNullableField("value", Schema.FieldType.INT64).build();

    Row row = SnowflakeSchemaTransformUtils.toRow(new String[] {""}, schema);

    assertThat(row.getValue("value"), equalTo(null));
  }

  @Test
  public void testEmptyRequiredStringIsPreserved() {
    Schema schema = Schema.builder().addStringField("value").build();

    Row row = SnowflakeSchemaTransformUtils.toRow(new String[] {""}, schema);

    assertThat(row.getString("value"), equalTo(""));
  }

  @Test
  public void testEmptyRequiredNonStringIsRejected() {
    Schema schema = Schema.builder().addInt64Field("value").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toRow(new String[] {""}, schema));

    assertThat(
        exception.getMessage(),
        equalTo("Received an empty value for non-nullable Snowflake field 'value'."));
  }

  @Test
  public void testWrongNumberOfFieldsIsRejected() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toRow(new String[] {"1"}, schema));

    assertThat(
        exception.getMessage(),
        equalTo("Snowflake row contains 1 values, but the configured schema contains 2 fields."));
  }

  @Test
  public void testArrayIsRejected() {
    Schema schema = Schema.builder().addArrayField("values", Schema.FieldType.STRING).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toRow(new String[] {"value"}, schema));

    assertThat(
        exception.getMessage(),
        equalTo("Unable to parse value 'value' as ARRAY for Snowflake field 'values'."));
  }

  @Test
  public void testOauthAuthenticationIsValid() {
    provider.from(
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setOauthToken("token")
            .setTable("table")
            .build());
  }

  @Test
  public void testPrivateKeyAuthenticationIsValid() {
    provider.from(
        validConfiguration()
            .setPassword(null)
            .setPrivateKey("private-key")
            .setPrivateKeyPassphrase("passphrase")
            .setTable("table")
            .build());
  }

  @Test
  public void testAuthenticationMethodIsRequired() {
    Configuration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setOauthToken(null)
            .setPrivateKey(null)
            .setTable("table")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Exactly one authentication method must be configured: "
                + "password, oauthToken, or privateKey."));
  }

  @Test
  public void testMultipleAuthenticationMethodsAreRejected() {
    Configuration configuration =
        validConfiguration().setOauthToken("token").setTable("table").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Exactly one authentication method must be configured: "
                + "password, oauthToken, or privateKey."));
  }

  @Test
  public void testUsernameIsRequiredForPrivateKeyAuthentication() {
    Configuration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setPrivateKey("private-key")
            .setTable("table")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo("username is required for password and private key authentication."));
  }

  @Test
  public void testPrivateKeyPassphraseRequiresPrivateKey() {
    Configuration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setOauthToken("token")
            .setPrivateKeyPassphrase("passphrase")
            .setTable("table")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("privateKeyPassphrase requires privateKey."));
  }

  private static Configuration.Builder validConfiguration() {
    return Configuration.builder()
        .setServerName("account.snowflakecomputing.com")
        .setUsername("username")
        .setPassword("password")
        .setDatabase("database")
        .setSnowflakeSchema("public")
        .setWarehouse("warehouse")
        .setRole("role")
        .setStagingBucketName("gs://bucket/staging/")
        .setStorageIntegrationName("storage_integration")
        .setSchema(SIMPLE_SCHEMA);
  }
}
