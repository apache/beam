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
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvPayloadSerializer}. */
@RunWith(JUnit4.class)
public class CsvPayloadSerializerTest {

  @Test
  public void serializeWithDefaultCsvFormat() {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(ALL_DATA_TYPES_SCHEMA, null);
    byte[] actualBytes =
        payloadSerializer.serialize(
            rowOf(
                true,
                (byte) 0,
                Instant.ofEpochMilli(1670358365856L).toDateTime(),
                BigDecimal.valueOf(1L),
                3.12345,
                4.1f,
                (short) 5,
                2,
                7L,
                "asdfjkl;"));
    String actual = new String(actualBytes, StandardCharsets.UTF_8);
    assertEquals("asdfjkl;,true,0,2022-12-06T20:26:05.856Z,1,3.12345,4.1,5,2,7", actual);
  }

  @Test
  public void serializeWithPostGreSQLFormat() {
    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(ALL_DATA_TYPES_SCHEMA, CSVFormat.POSTGRESQL_CSV);
    byte[] actualBytes =
        payloadSerializer.serialize(
            rowOf(
                true,
                (byte) 0,
                Instant.ofEpochMilli(1670358365856L).toDateTime(),
                BigDecimal.valueOf(1L),
                3.12345,
                4.1f,
                (short) 5,
                2,
                7L,
                "asdfjkl;"));
    String actual = new String(actualBytes, StandardCharsets.UTF_8);
    assertEquals(
        "\"asdfjkl;\",\"true\",\"0\",\"2022-12-06T20:26:05.856Z\",\"1\",\"3.12345\",\"4.1\",\"5\",\"2\",\"7\"",
        actual);
  }

  @Test
  public void deserialize() {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(ALL_DATA_TYPES_SCHEMA, null);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          payloadSerializer.deserialize(new byte[] {});
        });
  }
}
