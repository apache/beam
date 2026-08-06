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

import org.apache.beam.sdk.io.snowflake.SnowflakeWriteSchemaTransformProvider.Configuration;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeWriteSchemaTransformProviderTest {

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
    Configuration configuration = validConfiguration().build();

    provider.from(configuration);
  }

  @Test
  public void testBlankQuotationMarkIsAllowed() {
    Configuration configuration = validConfiguration().setQuotationMark("").build();

    provider.from(configuration);
  }

  @Test
  public void testMissingServerName() {
    Configuration configuration =
        Configuration.builder()
            .setUsername("username")
            .setPassword("password")
            .setDatabase("database")
            .setSchema("schema")
            .setTable("table")
            .setServerName("")
            .setStagingBucketName("gs://bucket/staging/")
            .setStorageIntegrationName("storage_integration")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("serverName cannot be empty"));
  }

  @Test
  public void testMissingTable() {
    Configuration configuration =
        Configuration.builder()
            .setServerName("account.snowflakecomputing.com")
            .setUsername("username")
            .setPassword("password")
            .setDatabase("database")
            .setSchema("schema")
            .setTable("")
            .setStagingBucketName("gs://bucket/staging/")
            .setStorageIntegrationName("storage_integration")
            .build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("table cannot be empty"));
  }

  @Test
  public void testStagingBucketMustEndWithSlash() {
    Configuration configuration =
        validConfiguration().setStagingBucketName("gs://bucket/staging").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(exception.getMessage(), equalTo("stagingBucketName must end with '/'"));
  }

  @Test
  public void testInvalidCreateDisposition() {
    Configuration configuration = validConfiguration().setCreateDisposition("INVALID").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Unsupported createDisposition 'INVALID'. Supported values are "
                + "CREATE_IF_NEEDED and CREATE_NEVER."));
  }

  @Test
  public void testInvalidWriteDisposition() {
    Configuration configuration = validConfiguration().setWriteDisposition("INVALID").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Unsupported writeDisposition 'INVALID'. Supported values are "
                + "APPEND, TRUNCATE, and EMPTY."));
  }

  @Test
  public void testSupportedWriteDispositions() {
    provider.from(validConfiguration().setWriteDisposition("APPEND").build());
    provider.from(validConfiguration().setWriteDisposition("TRUNCATE").build());
    provider.from(validConfiguration().setWriteDisposition("EMPTY").build());
  }

  @Test
  public void testCreateNeverIsSupported() {
    Configuration configuration = validConfiguration().setCreateDisposition("CREATE_NEVER").build();

    provider.from(configuration);
  }

  private static Configuration.Builder validConfiguration() {
    return Configuration.builder()
        .setServerName("account.snowflakecomputing.com")
        .setUsername("username")
        .setPassword("password")
        .setDatabase("database")
        .setSchema("schema")
        .setWarehouse("warehouse")
        .setRole("role")
        .setTable("table")
        .setStagingBucketName("gs://bucket/staging/")
        .setStorageIntegrationName("storage_integration");
  }

  @Test
  public void testConvertsBeamSchemaToSnowflakeSchema() {
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

    SnowflakeTableSchema snowflakeSchema =
        SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema);

    SnowflakeColumn[] columns = snowflakeSchema.getColumns();

    assertThat(columns.length, equalTo(10));

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
  public void testPreservesColumnNamesAndNullability() {
    Schema schema =
        Schema.builder()
            .addStringField("required_value")
            .addNullableField("optional_value", Schema.FieldType.INT64)
            .build();

    SnowflakeTableSchema snowflakeSchema =
        SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema);

    SnowflakeColumn[] columns = snowflakeSchema.getColumns();

    assertThat(columns[0].getName(), equalTo("required_value"));
    assertThat(columns[0].isNullable(), equalTo(false));

    assertThat(columns[1].getName(), equalTo("optional_value"));
    assertThat(columns[1].isNullable(), equalTo(true));
  }

  @Test
  public void testSnowflakeSchemaSql() {
    Schema schema = Schema.builder().addInt64Field("id").addNullableStringField("name").build();

    SnowflakeTableSchema snowflakeSchema =
        SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema);

    assertThat(snowflakeSchema.sql(), equalTo("id NUMBER(38,0), name VARCHAR NULL"));
  }

  @Test
  public void testDecimalIsRejected() {
    Schema schema = Schema.builder().addDecimalField("amount").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema));

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
            () -> SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema));

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
            () -> SnowflakeWriteSchemaTransformProvider.toSnowflakeTableSchema(schema));

    assertThat(
        exception.getMessage(),
        equalTo("Unsupported Beam field type ROW for Snowflake column 'nested'."));
  }

  @Test
  public void testCreateIfNeededIsSupported() {
    Configuration configuration =
        validConfiguration().setCreateDisposition("CREATE_IF_NEEDED").build();

    provider.from(configuration);
  }
}
