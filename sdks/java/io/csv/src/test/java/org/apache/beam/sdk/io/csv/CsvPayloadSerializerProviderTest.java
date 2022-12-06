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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
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
        PayloadSerializers.getSerializer(PAYLOAD_SERIALIZER_ID, Schema.of(), ImmutableMap.of());
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.DEFAULT, csvPayloadSerializer.getCsvFormat());
  }

  @Test
  public void getSerializerWithNonDefaultCsvFormat() {
    PayloadSerializer payloadSerializer =
        PayloadSerializers.getSerializer(
            PAYLOAD_SERIALIZER_ID,
            Schema.of(),
            ImmutableMap.of(
                CsvPayloadSerializerProvider.CSV_FORMAT_PARAMETER_KEY, CSVFormat.MONGODB_CSV));
    assertTrue(payloadSerializer instanceof CsvPayloadSerializer);
    CsvPayloadSerializer csvPayloadSerializer = (CsvPayloadSerializer) payloadSerializer;
    assertEquals(CSVFormat.MONGODB_CSV, csvPayloadSerializer.getCsvFormat());
  }

  @Test
  public void invalidPayloadSerializerParameter() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PayloadSerializers.getSerializer(
              PAYLOAD_SERIALIZER_ID, Schema.of(), ImmutableMap.of("badparam", ""));
        });
  }

  @Test
  public void invalidPayloadSerializerCsvFormatTypeParameter() {
    assertThrows(
        ClassCastException.class,
        () -> {
          PayloadSerializers.getSerializer(
              PAYLOAD_SERIALIZER_ID,
              Schema.of(),
              ImmutableMap.of(CsvPayloadSerializerProvider.CSV_FORMAT_PARAMETER_KEY, ""));
        });
  }
}
