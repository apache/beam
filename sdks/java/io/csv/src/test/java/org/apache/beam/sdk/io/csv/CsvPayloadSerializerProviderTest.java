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
package org.apache.beam.sdk.io.csv;

import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.ALL_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvPayloadSerializerProvider.CSV_FORMAT_PARAMETER_FIELD;
import static org.apache.beam.sdk.io.csv.CsvPayloadSerializerProvider.CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvPayloadSerializerProvider.SCHEMA_FIELDS_PARAMETER_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvPayloadSerializerProvider}. */
@RunWith(JUnit4.class)
public class CsvPayloadSerializerProviderTest {

  @Test
  public void invalidSchema() {
    assertThrows(
        "should not allow an empty schema",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER, Schema.of(), ImmutableMap.of()));

    assertThrows(
        "should not allow row field",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER,
                Schema.of(
                    Field.of("badfield", FieldType.row(ALL_DATA_TYPES_SCHEMA)),
                    Field.of("ok", FieldType.STRING)),
                ImmutableMap.of()));

    assertThrows(
        "should not allow array field",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER,
                Schema.of(
                    Field.of("badfield", FieldType.array(FieldType.INT16)),
                    Field.of("ok", FieldType.STRING)),
                ImmutableMap.of()));

    assertThrows(
        "should not allow map field",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER,
                Schema.of(
                    Field.of("badfield", FieldType.map(FieldType.INT16, FieldType.STRING)),
                    Field.of("ok", FieldType.STRING)),
                ImmutableMap.of()));
  }

  @Test
  public void invalidSchemaFieldNames() {
    assertThrows(
        "should not allow field names not included in the schema",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER,
                ALL_DATA_TYPES_SCHEMA,
                ImmutableMap.of(
                    SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("a", "b", "c"))));

    assertThrows(
        "should not allow empty field names list",
        IllegalArgumentException.class,
        () ->
            PayloadSerializers.getSerializer(
                CsvPayloadSerializerProvider.IDENTIFIER,
                ALL_DATA_TYPES_SCHEMA,
                ImmutableMap.of(SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Collections.emptyList())));
  }

  @Test
  public void getSerializerWithDefaultCsvFormat() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(
            CsvPayloadSerializerProvider.IDENTIFIER, ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.DEFAULT, csvPayloadSerializer.getCsvFormat());
  }

  @Test
  public void getSerializerWithNonDefaultCsvFormat() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(
            CsvPayloadSerializerProvider.IDENTIFIER,
            ALL_DATA_TYPES_SCHEMA,
            ImmutableMap.of(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.ORACLE));
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.ORACLE, csvPayloadSerializer.getCsvFormat());
    assertEquals(
        ALL_DATA_TYPES_SCHEMA.sorted().getFieldNames(), csvPayloadSerializer.getSchemaFields());
  }

  @Test
  public void getSerializerWithNonDefaultSchemaFields() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(
            CsvPayloadSerializerProvider.IDENTIFIER,
            ALL_DATA_TYPES_SCHEMA,
            ImmutableMap.of(
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(),
                Arrays.asList("aShort", "dateTime", "anInt")));
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.DEFAULT, csvPayloadSerializer.getCsvFormat());
    assertEquals(
        Arrays.asList("aShort", "dateTime", "anInt"), csvPayloadSerializer.getSchemaFields());
  }

  @Test
  public void getSerializerWithNonDefaults() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(
            CsvPayloadSerializerProvider.IDENTIFIER,
            ALL_DATA_TYPES_SCHEMA,
            ImmutableMap.of(
                CSV_FORMAT_PARAMETER_FIELD.getName(),
                CSVFormat.ORACLE,
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(),
                Arrays.asList("aShort", "dateTime", "anInt")));
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.ORACLE, csvPayloadSerializer.getCsvFormat());
    assertEquals(
        Arrays.asList("aShort", "dateTime", "anInt"), csvPayloadSerializer.getSchemaFields());
  }

  @Test
  public void rowFromDefaultParameters() {
    assertEquals(
        Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA).build(),
        CsvPayloadSerializerProvider.rowFrom(ImmutableMap.of()));
  }

  @Test
  public void rowFromNonDefaultCsvFormat() {
    assertEquals(
        Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
            .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.MONGODB_CSV)
            .withFieldValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName(), null)
            .build(),
        CsvPayloadSerializerProvider.rowFrom(
            ImmutableMap.of(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.MONGODB_CSV)));
  }

  @Test
  public void rowFromSchemaFields() {
    assertEquals(
        Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
            .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), null)
            .withFieldValue(
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble"))
            .build(),
        CsvPayloadSerializerProvider.rowFrom(
            ImmutableMap.of(
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble"))));
  }

  @Test
  public void rowFromNonDefaultParameters() {
    assertEquals(
        Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
            .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.INFORMIX_UNLOAD)
            .withFieldValue(
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble"))
            .build(),
        CsvPayloadSerializerProvider.rowFrom(
            ImmutableMap.of(
                CSV_FORMAT_PARAMETER_FIELD.getName(),
                CSVFormat.INFORMIX_UNLOAD,
                SCHEMA_FIELDS_PARAMETER_FIELD.getName(),
                Arrays.asList("string", "aDouble"))));
  }
}
