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
import java.util.HashMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Predefined;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvPayloadSerializerProvider}. */
@RunWith(JUnit4.class)
public class CsvPayloadSerializerProviderTest {
  private static final String PAYLOAD_SERIALIZER_ID = "csv";

  @Test
  public void getSerializerWithDefaultCsvFormat() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(PAYLOAD_SERIALIZER_ID, ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.DEFAULT, csvPayloadSerializer.getCsvFormat());
  }

  @Test
  public void rowFrom() {
    assertEquals(Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA).build(), CsvPayloadSerializerProvider.rowFrom(ImmutableMap.of()));

    assertEquals(Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
        .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.MONGODB_CSV)
        .withFieldValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName(), null)
        .build(), CsvPayloadSerializerProvider.rowFrom(ImmutableMap.of(
        CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.MONGODB_CSV
    )));

    assertEquals(Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
        .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), null)
        .withFieldValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble")).build(),
        CsvPayloadSerializerProvider.rowFrom(ImmutableMap.of(
            SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble")
        )));

    assertEquals(Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA)
            .withFieldValue(CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.INFORMIX_UNLOAD)
            .withFieldValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble")).build(),
        CsvPayloadSerializerProvider.rowFrom(ImmutableMap.of(
            CSV_FORMAT_PARAMETER_FIELD.getName(), CSVFormat.INFORMIX_UNLOAD,
            SCHEMA_FIELDS_PARAMETER_FIELD.getName(), Arrays.asList("string", "aDouble")
        )));
  }
}
