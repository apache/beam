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
package org.apache.beam.sdk.io.gcp.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTES;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.schemas.Schema.TypeName.MAP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.bigtable.v2.Cell;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class CellValueParserTest {

  private static final CellValueParser PARSER = new CellValueParser();

  @Test
  public void shouldParseBooleanTypeFalse() {
    byte[] value = new byte[] {0};
    assertEquals(false, PARSER.getCellValue(cell(value), BOOLEAN));
  }

  @Test
  public void shouldParseBooleanTypeTrueNotOne() {
    byte[] value = new byte[] {1};
    assertEquals(true, PARSER.getCellValue(cell(value), BOOLEAN));
  }

  @Test
  public void shouldParseBooleanTypeTrueOne() {
    byte[] value = new byte[] {4};
    assertEquals(true, PARSER.getCellValue(cell(value), BOOLEAN));
  }

  @Test
  public void shouldFailParseBooleanTypeTooLong() {
    byte[] value = new byte[10];
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), BOOLEAN));
    checkMessage(exception.getMessage(), "Boolean has to be 1-byte long bytearray");
  }

  @Test
  public void shouldParseByteType() {
    byte[] value = new byte[] {2};
    assertEquals((byte) 2, PARSER.getCellValue(cell(value), BYTE));
  }

  @Test
  public void shouldFailTooLongByteValue() {
    byte[] value = new byte[3];
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), BYTE));
    checkMessage(exception.getMessage(), "Byte has to be 1-byte long bytearray");
  }

  @Test
  public void shouldParseInt16Type() {
    byte[] value = new byte[] {2, 0};
    assertEquals((short) 512, PARSER.getCellValue(cell(value), INT16));
  }

  @Test
  public void shouldFailParseInt16TypeTooLong() {
    byte[] value = new byte[6];
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), INT16));
    checkMessage(exception.getMessage(), "Int16 has to be 2-bytes long bytearray");
  }

  @Test
  public void shouldParseInt32Type() {
    byte[] value = new byte[] {0, 2, 0, 0};
    assertEquals(131072, PARSER.getCellValue(cell(value), INT32));
  }

  @Test
  public void shouldFailParseInt32TypeTooLong() {
    byte[] value = new byte[6];
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), INT32));
    checkMessage(exception.getMessage(), "Int32 has to be 4-bytes long bytearray");
  }

  @Test
  public void shouldParseInt64Type() {
    byte[] value = new byte[] {0, 0, 0, 0, 0, 2, 0, 0};
    assertEquals(131072L, PARSER.getCellValue(cell(value), INT64));
  }

  @Test
  public void shouldFailParseInt64TypeTooLong() {
    byte[] value = new byte[10];
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), INT64));
    checkMessage(exception.getMessage(), "Int64 has to be 8-bytes long bytearray");
  }

  @Test
  public void shouldParseFloatType() {
    byte[] value = new byte[] {64, 0, 0, 0};
    assertEquals(2.0f, (Float) PARSER.getCellValue(cell(value), FLOAT), 0.001);
  }

  @Test
  public void shouldFailParseFloatTypeTooLong() {
    byte[] value = new byte[10];
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), FLOAT));
    checkMessage(exception.getMessage(), "Float has to be 4-bytes long bytearray");
  }

  @Test
  public void shouldParseDoubleType() {
    byte[] value = new byte[] {64, 21, 51, 51, 51, 51, 51, 51};
    assertEquals(5.3, (Double) PARSER.getCellValue(cell(value), DOUBLE), 0.001);
  }

  @Test
  public void shouldFailParseDoubleTypeTooLong() {
    byte[] value = new byte[10];
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> PARSER.getCellValue(cell(value), DOUBLE));
    checkMessage(exception.getMessage(), "Double has to be 8-bytes long bytearray");
  }

  @Test
  public void shouldParseStringType() {
    byte[] value = "stringValue".getBytes(UTF_8);
    assertEquals("stringValue", PARSER.getCellValue(cell(value), STRING));
  }

  @Test
  public void shouldParseDateType() {
    byte[] value = "2019-04-06".getBytes(UTF_8);
    assertEquals(DateTime.parse("2019-04-06"), PARSER.getCellValue(cell(value), DATETIME));
  }

  @Test
  public void shouldParseDatetimeType() {
    byte[] value = "2010-06-30T01:20".getBytes(UTF_8);
    assertEquals(new DateTime(2010, 6, 30, 1, 20), PARSER.getCellValue(cell(value), DATETIME));
  }

  @Test
  public void shouldParseBytesType() {
    byte[] value = new byte[] {1, 2, 3, 4, 5};
    assertEquals(
        ByteString.copyFrom(value),
        ByteString.copyFrom((byte[]) PARSER.getCellValue(cell(value), BYTES)));
  }

  @Test
  public void shouldFailOnUnsupportedType() {
    byte[] value = new byte[0];
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PARSER.getCellValue(cell(value), FieldType.of(MAP)));
    checkMessage(exception.getMessage(), "Unsupported cell value type 'MAP'.");
  }

  @Test
  public void shouldEncodeBooleanTrue() {
    ByteString expected = byteString(new byte[] {1});
    ByteString actual = PARSER.valueToByteString(true, BOOLEAN);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeBooleanFalse() {
    ByteString expected = byteString(new byte[] {0});
    ByteString actual = PARSER.valueToByteString(false, BOOLEAN);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeLong() {
    ByteString expected = byteString(new byte[] {0, 0, 0, 0, 0, 0, 0, 22});
    ByteString actual = PARSER.valueToByteString(22L, INT64);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeInt() {
    ByteString expected = byteString(new byte[] {0, 0, 0, 15});
    ByteString actual = PARSER.valueToByteString(15, INT32);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeShort() {
    ByteString expected = byteString(new byte[] {0, 5});
    ByteString actual = PARSER.valueToByteString((short) 5, INT16);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeByte() {
    ByteString expected = byteString(new byte[] {5});
    ByteString actual = PARSER.valueToByteString((byte) 5, BYTE);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeDouble() {
    ByteString expected = byteString(new byte[] {64, 21, 51, 51, 51, 51, 51, 51});
    ByteString actual = PARSER.valueToByteString(5.3d, DOUBLE);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeFloat() {
    ByteString expected = byteString(new byte[] {64, 0, 0, 0});
    ByteString actual = PARSER.valueToByteString(2.0f, FLOAT);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeUtf8String() {
    ByteString expected = byteString(new byte[] {'b', 'e', 'a', 'm'});
    ByteString actual = PARSER.valueToByteString("beam", STRING);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeByteArray() {
    ByteString expected = byteString(new byte[] {0, 2, 1});
    ByteString actual = PARSER.valueToByteString(new byte[] {0, 2, 1}, BYTES);
    assertEquals(expected, actual);
  }

  @Test
  public void shouldEncodeDateTime() {
    ByteString expected = byteString("2020-12-06T00:00:00.000Z".getBytes(UTF_8));
    ByteString actual =
        PARSER.valueToByteString(new DateTime(2020, 12, 6, 0, 0, 0, DateTimeZone.UTC), DATETIME);
    assertEquals(expected, actual);
  }

  private ByteString byteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  private Cell cell(byte[] value) {
    return Cell.newBuilder().setValue(ByteString.copyFrom(value)).build();
  }

  private void checkMessage(@Nullable String message, String substring) {
    if (message != null) {
      assertThat(message, containsString(substring));
    } else {
      fail();
    }
  }
}
