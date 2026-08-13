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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeWriteSchemaTransformProviderTest {

  private final transient Pipeline pipeline = Pipeline.create();

  private final SnowflakeWriteSchemaTransformProvider provider =
      new SnowflakeWriteSchemaTransformProvider();

  @Test
  public void testIdentifier() {
    assertThat(
        provider.identifier(), equalTo("beam:schematransform:org.apache.beam:snowflake_write:v1"));
  }

  @Test
  public void testInputCollectionNames() {
    assertThat(provider.inputCollectionNames(), contains("input"));
  }

  @Test
  public void testOutputCollectionNames() {
    assertThat(provider.outputCollectionNames(), empty());
  }

  @Test
  public void testValidConfiguration() {
    provider.from(validConfiguration().build());
  }

  @Test
  public void testBlankQuotationMarkIsAllowed() {
    provider.from(validConfiguration().setQuotationMark("").build());
  }

  @Test
  public void testMissingServerNameIsRejected() {
    SnowflakeWriteConfiguration configuration = validConfiguration().setServerName("").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("serverName cannot be empty"));
  }

  @Test
  public void testMissingTableIsAllowedAtConfigurationTime() {
    // Whether table is required depends on whether the input is bounded.
    provider.from(validConfiguration().setTable(null).build());
  }

  @Test
  public void testStagingBucketMustEndWithSlash() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration().setStagingBucketName("gs://bucket/staging").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("stagingBucketName must end with '/'"));
  }

  @Test
  public void testInvalidCreateDispositionIsRejected() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration().setCreateDisposition("INVALID").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Unsupported createDisposition 'INVALID'. "
                + "Supported values are CREATE_IF_NEEDED and CREATE_NEVER."));
  }

  @Test
  public void testCreateIfNeededIsAccepted() {
    provider.from(validConfiguration().setCreateDisposition("CREATE_IF_NEEDED").build());
  }

  @Test
  public void testCreateNeverIsAccepted() {
    provider.from(validConfiguration().setCreateDisposition("CREATE_NEVER").build());
  }

  @Test
  public void testInvalidWriteDispositionIsRejected() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration().setWriteDisposition("INVALID").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Unsupported writeDisposition 'INVALID'. "
                + "Supported values are APPEND, TRUNCATE, and EMPTY."));
  }

  @Test
  public void testSupportedWriteDispositions() {
    provider.from(validConfiguration().setWriteDisposition("APPEND").build());

    provider.from(validConfiguration().setWriteDisposition("TRUNCATE").build());

    provider.from(validConfiguration().setWriteDisposition("EMPTY").build());
  }

  @Test
  public void testOauthAuthenticationIsValid() {
    provider.from(
        validConfiguration().setUsername(null).setPassword(null).setOauthToken("token").build());
  }

  @Test
  public void testPrivateKeyAuthenticationIsValid() {
    provider.from(
        validConfiguration()
            .setPassword(null)
            .setPrivateKey("private-key")
            .setPrivateKeyPassphrase("passphrase")
            .build());
  }

  @Test
  public void testPrivateKeyWithoutPassphraseIsValid() {
    provider.from(validConfiguration().setPassword(null).setPrivateKey("private-key").build());
  }

  @Test
  public void testAuthenticationMethodIsRequired() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setOauthToken(null)
            .setPrivateKey(null)
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
    SnowflakeWriteConfiguration configuration = validConfiguration().setOauthToken("token").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Exactly one authentication method must be configured: "
                + "password, oauthToken, or privateKey."));
  }

  @Test
  public void testUsernameIsRequiredForPasswordAuthentication() {
    SnowflakeWriteConfiguration configuration = validConfiguration().setUsername(null).build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo("username is required for password and private key authentication."));
  }

  @Test
  public void testUsernameIsRequiredForPrivateKeyAuthentication() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setPrivateKey("private-key")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo("username is required for password and private key authentication."));
  }

  @Test
  public void testPrivateKeyPassphraseRequiresPrivateKey() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration()
            .setUsername(null)
            .setPassword(null)
            .setOauthToken("token")
            .setPrivateKeyPassphrase("passphrase")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("privateKeyPassphrase requires privateKey."));
  }

  @Test
  public void testInvalidDebugModeIsRejected() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration().setDebugMode("INVALID").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo("Unsupported debugMode 'INVALID'. " + "Supported values are ERROR and INFO."));
  }

  @Test
  public void testSupportedDebugModes() {
    provider.from(validConfiguration().setDebugMode("ERROR").build());

    provider.from(validConfiguration().setDebugMode("INFO").build());
  }

  @Test
  public void testFlushRowLimitMustBePositive() {
    SnowflakeWriteConfiguration configuration = validConfiguration().setFlushRowLimit(0).build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("flushRowLimit must be greater than 0."));
  }

  @Test
  public void testFlushTimeLimitMustBePositive() {
    SnowflakeWriteConfiguration configuration =
        validConfiguration().setFlushTimeLimitMillis(0L).build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("flushTimeLimitMillis must be greater than 0."));
  }

  @Test
  public void testShardsNumberMustBePositive() {
    SnowflakeWriteConfiguration configuration = validConfiguration().setShardsNumber(0).build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("shardsNumber must be greater than 0."));
  }

  @Test
  public void testBatchWriteRequiresTable() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    Row row = Row.withSchema(schema).addValues(1L, "Alice").build();

    PCollection<Row> rows =
        pipeline.apply(Create.of(row).withCoder(RowCoder.of(schema))).setRowSchema(schema);

    SnowflakeWriteConfiguration configuration = validConfiguration().setTable(null).build();

    SchemaTransform transform = provider.from(configuration);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> transform.expand(PCollectionRowTuple.of("input", rows)));

    assertThat(exception.getMessage(), equalTo("table is required for batch writes."));
  }

  @Test
  public void testStreamingWriteRequiresSnowPipe() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    Row row = Row.withSchema(schema).addValues(1L, "Alice").build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(schema)).addElements(row).advanceWatermarkToInfinity();

    PCollection<Row> rows = pipeline.apply(stream).setRowSchema(schema);

    SnowflakeWriteConfiguration configuration =
        validConfiguration().setTable(null).setSnowPipe(null).build();

    SchemaTransform transform = provider.from(configuration);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> transform.expand(PCollectionRowTuple.of("input", rows)));

    assertThat(exception.getMessage(), equalTo("snowPipe is required for streaming writes."));
  }

  @Test
  public void testStreamingConfigurationIsAccepted() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    Row row = Row.withSchema(schema).addValues(1L, "Alice").build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(schema)).addElements(row).advanceWatermarkToInfinity();

    PCollection<Row> rows = pipeline.apply(stream).setRowSchema(schema);

    SnowflakeWriteConfiguration configuration =
        validConfiguration()
            .setTable(null)
            .setSnowPipe("MY_PIPE")
            .setFlushRowLimit(50000)
            .setFlushTimeLimitMillis(18000L)
            .setShardsNumber(1)
            .setDebugMode("ERROR")
            .build();

    SchemaTransform transform = provider.from(configuration);

    transform.expand(PCollectionRowTuple.of("input", rows));
  }

  @Test
  public void testScalarSchemaMapping() {
    Schema schema =
        Schema.builder()
            .addByteField("byte_value")
            .addInt16Field("int16_value")
            .addInt32Field("int32_value")
            .addInt64Field("int64_value")
            .addFloatField("float_value")
            .addDoubleField("double_value")
            .addStringField("string_value")
            .addBooleanField("boolean_value")
            .addByteArrayField("bytes_value")
            .addDateTimeField("datetime_value")
            .build();

    SnowflakeTableSchema snowflakeSchema =
        SnowflakeSchemaTransformUtils.toSnowflakeTableSchema(schema);

    SnowflakeColumn[] columns = snowflakeSchema.getColumns();

    assertThat(columns[0].getDataType(), instanceOf(SnowflakeNumber.class));
    assertThat(columns[1].getDataType(), instanceOf(SnowflakeNumber.class));
    assertThat(columns[2].getDataType(), instanceOf(SnowflakeNumber.class));
    assertThat(columns[3].getDataType(), instanceOf(SnowflakeNumber.class));
    assertThat(columns[4].getDataType(), instanceOf(SnowflakeDouble.class));
    assertThat(columns[5].getDataType(), instanceOf(SnowflakeDouble.class));
    assertThat(columns[6].getDataType(), instanceOf(SnowflakeVarchar.class));
    assertThat(columns[7].getDataType(), instanceOf(SnowflakeBoolean.class));
    assertThat(columns[8].getDataType(), instanceOf(SnowflakeBinary.class));
    assertThat(columns[9].getDataType(), instanceOf(SnowflakeTimestamp.class));
  }

  @Test
  public void testSchemaMappingPreservesNameAndNullability() {
    Schema schema =
        Schema.builder()
            .addInt64Field("id")
            .addNullableField("name", Schema.FieldType.STRING)
            .build();

    SnowflakeTableSchema snowflakeSchema =
        SnowflakeSchemaTransformUtils.toSnowflakeTableSchema(schema);

    SnowflakeColumn[] columns = snowflakeSchema.getColumns();

    assertThat(columns[0].getName(), equalTo("id"));
    assertThat(columns[0].isNullable(), equalTo(false));

    assertThat(columns[1].getName(), equalTo("name"));
    assertThat(columns[1].isNullable(), equalTo(true));

    assertThat(snowflakeSchema.sql(), equalTo("id NUMBER(38,0), name VARCHAR NULL"));
  }

  @Test
  public void testDecimalIsRejected() {
    Schema schema = Schema.builder().addDecimalField("amount").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toSnowflakeTableSchema(schema));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Unsupported Beam field type DECIMAL for Snowflake column 'amount'. "
                + "Beam DECIMAL does not include Snowflake precision and scale information."));
  }

  @Test
  public void testArrayIsRejected() {
    Schema schema = Schema.builder().addArrayField("values", Schema.FieldType.STRING).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toSnowflakeTableSchema(schema));

    assertThat(
        exception.getMessage(),
        equalTo("Unsupported Beam field type ARRAY for Snowflake column 'values'."));
  }

  @Test
  public void testNestedRowIsRejected() {
    Schema nestedSchema = Schema.builder().addStringField("value").build();

    Schema schema = Schema.builder().addRowField("nested", nestedSchema).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeSchemaTransformUtils.toSnowflakeTableSchema(schema));

    assertThat(
        exception.getMessage(),
        equalTo("Unsupported Beam field type ROW for Snowflake column 'nested'."));
  }

  private static SnowflakeWriteConfiguration.Builder validConfiguration() {
    return SnowflakeWriteConfiguration.builder()
        .setServerName("account.snowflakecomputing.com")
        .setUsername("username")
        .setPassword("password")
        .setDatabase("database")
        .setSchema("public")
        .setWarehouse("warehouse")
        .setRole("role")
        .setTable("table")
        .setStagingBucketName("gs://bucket/staging/")
        .setStorageIntegrationName("storage_integration");
  }
}
