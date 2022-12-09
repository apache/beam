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
import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.rowOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CsvPayloadSerializer}.
 */
@RunWith(JUnit4.class)
public class CsvPayloadSerializerTest {

  final Row row = rowOf(
      false,
      (byte) 1,
      Instant.ofEpochMilli(1670537248873L).toDateTime(),
      BigDecimal.valueOf(123456789L),
      1.23456789,
      1.23f,
      (short) 2,
      3,
      999999L,
      "abcdefg"
  );

  final Schema schema = row.getSchema();

  @Test
  public void serializeWithDefaultCSVFormat() {
    PayloadSerializer payloadSerializer = new CsvPayloadSerializer(schema, null, null);
    assertEquals("false,1,1.23456789,1.23,999999,2,3,2022-12-08T22:07:28.873Z,123456789,abcdefg", new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8));
  }

  @Test
  public void serializeWithNonDefaultCSVFormat() {
    PayloadSerializer payloadSerializer = new CsvPayloadSerializer(schema, CSVFormat.POSTGRESQL_CSV, null);
    assertEquals("\"false\",\"1\",\"1.23456789\",\"1.23\",\"999999\",\"2\",\"3\",\"2022-12-08T22:07:28.873Z\",\"123456789\",\"abcdefg\"", new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8));
  }

  @Test
  public void serializeWithDefaultCSVFormatAndFieldSubset() {
    PayloadSerializer payloadSerializer = new CsvPayloadSerializer(schema, null, Arrays.asList("string", "aDouble", "anInt"));
    assertEquals("abcdefg,1.23456789,3", new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8));
  }

  @Test
  public void serializeWithNonDefaultCSVFormatAndFieldSubset() {
    PayloadSerializer payloadSerializer = new CsvPayloadSerializer(schema, CSVFormat.MYSQL, Arrays.asList("string", "aDouble", "anInt"));
    assertEquals("abcdefg\t1.23456789\t3", new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8));
  }

  @Test
  public void invalidSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(Schema.of(Field.of("badfield", FieldType.BYTES), Field.of("ok", FieldType.STRING)), null, null)
    );

    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(Schema.of(Field.of("badfield", FieldType.array(FieldType.INT16)), Field.of("ok", FieldType.STRING)), null, null)
    );

    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(Schema.of(Field.of("badfield", FieldType.map(FieldType.BOOLEAN, FieldType.STRING)), Field.of("ok", FieldType.STRING)), null, null)
    );

    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(Schema.of(Field.of("badfield", FieldType.row(schema)), Field.of("ok", FieldType.STRING)), null, null)
    );

    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(Schema.of(), null, null)
    );
  }

  @Test
  public void invalidHeaderAgainstSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvPayloadSerializer(schema, null, Collections.emptyList())
    );
  }

  @Test
  public void deserialize() {
    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(ALL_DATA_TYPES_SCHEMA, null, null);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          payloadSerializer.deserialize(new byte[]{});
        });
  }
}
