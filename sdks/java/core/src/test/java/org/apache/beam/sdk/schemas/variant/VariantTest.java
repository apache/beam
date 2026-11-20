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
package org.apache.beam.sdk.schemas.variant;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Variant}, including conformance against the official vectors from <a
 * href="https://github.com/apache/parquet-testing/tree/master/variant">apache/parquet-testing</a>.
 */
@RunWith(JUnit4.class)
public class VariantTest {

  private static byte[] resource(String fileName) {
    String name = fileName.substring(0, fileName.lastIndexOf('.'));
    return fileName.endsWith(".metadata")
        ? ParquetTestingVectors.metadata(name)
        : ParquetTestingVectors.value(name);
  }

  private static Variant vector(String name) {
    return Variant.of(ParquetTestingVectors.metadata(name), ParquetTestingVectors.value(name));
  }

  /** Re-encoding a primitive vector must reproduce its exact bytes (canonical encoding). */
  private static void assertCanonicalRoundTrip(String name) {
    Variant original = vector(name);
    Variant reencoded = VariantBuilder.encode(original);
    assertEquals(name, original, reencoded);
    assertArrayEquals(name + " metadata", resource(name + ".metadata"), reencoded.metadata());
    assertArrayEquals(name + " value", resource(name + ".value"), reencoded.value());
  }

  // ---------------------------------------------------------------------------------------------
  // parquet-testing conformance vectors
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConformancePrimitives() {
    assertTrue(vector("primitive_null").isVariantNull());
    assertTrue(vector("primitive_boolean_true").getBoolean());
    assertFalse(vector("primitive_boolean_false").getBoolean());

    assertEquals(VariantType.INT8, vector("primitive_int8").getType());
    assertEquals(42L, vector("primitive_int8").getLong());
    assertEquals(VariantType.INT16, vector("primitive_int16").getType());
    assertEquals(1234L, vector("primitive_int16").getLong());
    assertEquals(VariantType.INT32, vector("primitive_int32").getType());
    assertEquals(123456L, vector("primitive_int32").getLong());
    assertEquals(VariantType.INT64, vector("primitive_int64").getType());
    assertEquals(1234567890123456789L, vector("primitive_int64").getLong());

    assertEquals(VariantType.DOUBLE, vector("primitive_double").getType());
    assertEquals(1234567890.1234, vector("primitive_double").getDouble(), 0.0);
    assertEquals(VariantType.FLOAT, vector("primitive_float").getType());
    assertEquals(1234567940.0f, vector("primitive_float").getFloat(), 0.0f);

    assertEquals(VariantType.DECIMAL4, vector("primitive_decimal4").getType());
    assertEquals(new BigDecimal("12.34"), vector("primitive_decimal4").getDecimal());
    assertEquals(VariantType.DECIMAL8, vector("primitive_decimal8").getType());
    assertEquals(new BigDecimal("12345678.90"), vector("primitive_decimal8").getDecimal());
    assertEquals(VariantType.DECIMAL16, vector("primitive_decimal16").getType());
    assertEquals(
        new BigDecimal("12345678912345678.90"), vector("primitive_decimal16").getDecimal());

    assertEquals(LocalDate.of(2025, 4, 16), vector("primitive_date").getDate());
    assertEquals(
        Instant.parse("2025-04-16T16:34:56.780Z"), vector("primitive_timestamp").getTimestamp());
    assertEquals(
        LocalDateTime.parse("2025-04-16T12:34:56.780"),
        vector("primitive_timestampntz").getTimestampNtz());
    assertEquals(VariantType.TIMESTAMP_NANOS, vector("primitive_timestamp_nanos").getType());
    assertEquals(
        Instant.parse("2024-11-07T12:33:54.123456789Z"),
        vector("primitive_timestamp_nanos").getTimestamp());
    assertEquals(VariantType.TIMESTAMP_NTZ_NANOS, vector("primitive_timestampntz_nanos").getType());
    assertEquals(
        LocalDateTime.parse("2024-11-07T12:33:54.123456789"),
        vector("primitive_timestampntz_nanos").getTimestampNtz());
    assertEquals(LocalTime.parse("12:33:54.123456"), vector("primitive_time").getTime());
    assertEquals(
        UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
        vector("primitive_uuid").getUuid());

    assertArrayEquals(
        Base64.getDecoder().decode("AxM33q2+78r+"), vector("primitive_binary").getBinary());
    assertEquals("Less than 64 bytes (\u2764\ufe0f with utf8)", vector("short_string").getString());
    assertTrue(vector("primitive_string").getString().startsWith("This string is longer than 64"));
    assertTrue(vector("primitive_string").getString().endsWith("\ud83e\udd26!!"));
    assertEquals(VariantType.STRING, vector("long_string").getType());
  }

  @Test
  public void testConformancePrimitivesReencodeToIdenticalBytes() {
    for (String name :
        Arrays.asList(
            "primitive_null",
            "primitive_boolean_true",
            "primitive_boolean_false",
            "primitive_int8",
            "primitive_int16",
            "primitive_int32",
            "primitive_int64",
            "primitive_double",
            "primitive_float",
            "primitive_decimal4",
            "primitive_decimal8",
            "primitive_decimal16",
            "primitive_date",
            "primitive_timestamp",
            "primitive_timestampntz",
            "primitive_timestamp_nanos",
            "primitive_timestampntz_nanos",
            "primitive_time",
            "primitive_uuid",
            "primitive_binary",
            "primitive_string",
            "short_string",
            "long_string")) {
      assertCanonicalRoundTrip(name);
    }
  }

  @Test
  public void testConformanceObjects() {
    assertEquals(0, vector("object_empty").objectSize());
    assertEquals("{}", vector("object_empty").toJson());

    Variant primitive = vector("object_primitive");
    assertEquals(VariantType.OBJECT, primitive.getType());
    assertEquals(7, primitive.objectSize());
    assertEquals(1L, primitive.getField("int_field").getLong());
    assertEquals("Apache Parquet", primitive.getField("string_field").getString());
    assertTrue(primitive.getField("null_field").isVariantNull());
    assertTrue(primitive.getField("boolean_true_field").getBoolean());
    assertFalse(primitive.getField("boolean_false_field").getBoolean());
    assertNull(primitive.getField("no_such_field"));

    // Field values are not stored in key order in this vector; offsets must be honoured.
    Variant nested = vector("object_nested");
    assertEquals(
        Variant.parseJson(
            "{\"id\": 1, \"observation\": {\"location\": \"In the Volcano\", \"time\": \"12:34:56\","
                + " \"value\": {\"humidity\": 456, \"temperature\": 123}},"
                + " \"species\": {\"name\": \"lava monster\", \"population\": 6789}}"),
        nested);
    assertEquals(456L, nested.get("$.observation.value.humidity").getLong());
    assertEquals("lava monster", nested.get("$['species']['name']").getString());
  }

  @Test
  public void testConformanceArrays() {
    assertEquals(0, vector("array_empty").arraySize());
    assertEquals("[]", vector("array_empty").toJson());
    assertEquals(Variant.parseJson("[2, 1, 5, 9]"), vector("array_primitive"));

    Variant nested = vector("array_nested");
    assertEquals(
        Variant.parseJson(
            "[{\"id\": 1, \"thing\": {\"names\": [\"Contrarian\", \"Spider\"]}}, null,"
                + " {\"id\": 2, \"names\": [\"Apple\", \"Ray\", null], \"type\": \"if\"}]"),
        nested);
    assertEquals("Spider", nested.get("$[0].thing.names[1]").getString());
    assertTrue(nested.get("$[1]").isVariantNull());
    assertTrue(nested.get("$[2].names[2]").isVariantNull());
  }

  @Test
  public void testConformanceContainersSurviveReencoding() {
    for (String name :
        Arrays.asList(
            "object_empty",
            "object_primitive",
            "object_nested",
            "array_empty",
            "array_primitive",
            "array_nested")) {
      Variant original = vector(name);
      Variant reencoded = VariantBuilder.encode(original);
      assertEquals(name, original, reencoded);
      assertEquals(name, original.hashCode(), reencoded.hashCode());
      assertEquals(name, original.toJson(), reencoded.toJson());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // JSON
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testParseJsonAndPathAccess() {
    Variant v = Variant.parseJson("{\"event\":\"login\",\"device\":{\"os\":\"ios\"}}");
    assertEquals(VariantType.OBJECT, v.getType());
    assertEquals("ios", v.get("$.device.os").getString());
    assertEquals(ImmutableList.of("device", "event"), v.fieldNames());
    assertEquals("{\"device\":{\"os\":\"ios\"},\"event\":\"login\"}", v.toJson());
    assertNull(v.get("$.device.version"));
    assertNull(v.get("$.event.nested"));
    assertNull(v.get("$[0]"));
    assertEquals(v, v.get("$"));
  }

  @Test
  public void testJsonNumbersFollowSparkRules() {
    assertEquals(VariantType.INT8, Variant.parseJson("1").getType());
    assertEquals(VariantType.INT16, Variant.parseJson("300").getType());
    assertEquals(VariantType.INT32, Variant.parseJson("70000").getType());
    assertEquals(VariantType.INT64, Variant.parseJson("5000000000").getType());
    // Integers beyond int64 become exact decimals.
    Variant big = Variant.parseJson("12345678901234567890");
    assertEquals(VariantType.DECIMAL16, big.getType());
    assertEquals(new BigDecimal("12345678901234567890"), big.getDecimal());
    // Fractions become the smallest exact decimal, not double.
    Variant amount = Variant.parseJson("19.99");
    assertEquals(VariantType.DECIMAL4, amount.getType());
    assertEquals(new BigDecimal("19.99"), amount.getDecimal());
    assertEquals(VariantType.DECIMAL8, Variant.parseJson("1234567890.12").getType());
    // Exponents and > 38 digits become double.
    assertEquals(VariantType.DOUBLE, Variant.parseJson("1.5e3").getType());
    assertEquals(1500.0, Variant.parseJson("1.5e3").getDouble(), 0.0);
    assertEquals(VariantType.DOUBLE, Variant.parseJson("0." + "1".repeat(40)).getType());
    assertEquals("-0.50", Variant.parseJson("-0.50").toJson());
  }

  @Test
  public void testJsonScalarsAndStrings() {
    assertTrue(Variant.parseJson("null").isVariantNull());
    assertTrue(Variant.parseJson("true").getBoolean());
    assertEquals("x", Variant.parseJson("\"x\"").getString());
    String longString = "abc".repeat(100);
    Variant v = Variant.parseJson("\"" + longString + "\"");
    assertEquals(longString, v.getString());
    // No timestamp detection.
    assertEquals(VariantType.STRING, Variant.parseJson("\"2025-01-01\"").getType());
    // Escaping round trip.
    String tricky = "{\"q\":\"a\\\"b\\\\c\\n\\u00e9\"}";
    assertEquals(Variant.parseJson(tricky), Variant.parseJson(Variant.parseJson(tricky).toJson()));
  }

  @Test
  public void testInvalidJson() {
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("{\"a\":1,\"a\":2}"));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("{\"a\":"));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("1 2"));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson(""));
    assertNull(Variant.tryParseJson("{bad"));
    assertNull(Variant.tryParseJson(null));
    assertEquals(Variant.parseJson("1"), Variant.tryParseJson("1"));
  }

  @Test
  public void testPaths() {
    Variant v =
        Variant.parseJson(
            "{\"items\":[{\"sku\":\"a\"},{\"sku\":\"b\"}],\"a.b\":{\"c\":true},\"n\":null}");
    assertEquals("b", v.get("$.items[1].sku").getString());
    assertEquals("a", v.get("$['items'][0]['sku']").getString());
    assertTrue(v.get("$[\"a.b\"].c").getBoolean());
    assertTrue(v.get("$.n").isVariantNull());
    assertNull(v.get("$.items[2]"));
    assertNull(v.get("$.missing.deeper"));
    assertThrows(IllegalArgumentException.class, () -> v.get("items"));
    assertThrows(IllegalArgumentException.class, () -> v.get("$."));
    assertThrows(IllegalArgumentException.class, () -> v.get("$[x]"));
    assertThrows(IllegalArgumentException.class, () -> v.get("$['open"));
  }

  // ---------------------------------------------------------------------------------------------
  // Builders, equality, encoding edge cases
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testLogicalEquality() {
    // Same logical number in different physical types.
    Variant int8 = Variant.parseJson("1");
    Variant int64 = Variant.of(new byte[] {1, 0, 0}, new byte[] {0x18, 1, 0, 0, 0, 0, 0, 0, 0});
    Variant decimal = Variant.parseJson("1.00");
    assertEquals(VariantType.INT64, int64.getType());
    assertEquals(int8, int64);
    assertEquals(int8, decimal);
    assertEquals(int8.hashCode(), int64.hashCode());
    assertEquals(int8.hashCode(), decimal.hashCode());
    // ... but not equal to a double or a string.
    assertNotEquals(int8, Variant.parseJson("1e0"));
    assertNotEquals(int8, Variant.parseJson("\"1\""));

    // Field order in the input does not matter.
    Variant ab = Variant.parseJson("{\"a\":1,\"b\":[true,null]}");
    Variant ba = Variant.parseJson("{\"b\":[true,null],\"a\":1}");
    assertEquals(ab, ba);
    assertEquals(ab.hashCode(), ba.hashCode());
    assertArrayEquals(ab.value(), ba.value()); // canonical encoding
    assertNotEquals(ab, Variant.parseJson("{\"a\":1,\"b\":[true]}"));
    assertNotEquals(ab, Variant.parseJson("{\"a\":1,\"c\":[true,null]}"));
  }

  @Test
  public void testNestedValueSliceIsStandalone() {
    Variant v = Variant.parseJson("{\"a\":{\"x\":[1,2,3]},\"b\":\"str\"}");
    Variant a = v.getField("a");
    // A nested value shares the parent's metadata; its value() is the exact slice.
    Variant copy = Variant.of(a.metadata(), a.value());
    assertEquals(a, copy);
    assertEquals("{\"x\":[1,2,3]}", copy.toJson());
  }

  @Test
  public void testLargeContainersAndOffsets() {
    // > 255 fields: is_large objects, 2-byte field ids, binary search path.
    Map<String, Object> wide = new LinkedHashMap<>();
    for (int i = 0; i < 1000; i++) {
      wide.put(String.format("field_%04d", i), (long) i);
    }
    Variant v = VariantBuilder.encode(wide);
    assertEquals(1000, v.objectSize());
    assertEquals(0L, v.getField("field_0000").getLong());
    assertEquals(999L, v.getField("field_0999").getLong());
    assertEquals(512L, v.getField("field_0512").getLong());
    assertNull(v.getField("field_1000"));
    assertNull(v.getField("aaa"));

    // > 255 elements and > 64KiB of data: is_large arrays, 3-byte offsets.
    List<Object> list = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      list.add("x".repeat(300));
    }
    Variant arr = VariantBuilder.encode(list);
    assertEquals(300, arr.arraySize());
    assertEquals("x".repeat(300), arr.getElement(299).getString());
    assertEquals(arr, Variant.parseJson(arr.toJson()));
  }

  @Test
  public void testKeysAreSortedByUtf8ByteOrder() {
    // U+FFFD sorts before U+1F600 in UTF-8 / code point order, but after it in UTF-16
    // (String.compareTo), because U+1F600 is encoded as the surrogate pair D83D DE00.
    String bmp = "\uFFFD";
    String emoji = "\uD83D\uDE00";
    Variant small = VariantBuilder.encode(ImmutableMap.of(emoji, 2L, bmp, 1L, "a", 0L));
    assertEquals(ImmutableList.of("a", bmp, emoji), small.fieldNames());

    // Enough fields to use binary search in getField.
    Map<String, Object> wide = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      wide.put(String.format("k%03d", i), (long) i);
    }
    wide.put(bmp, "bmp");
    wide.put(emoji, "emoji");
    Variant v = VariantBuilder.encode(wide);
    assertEquals("bmp", v.getField(bmp).getString());
    assertEquals("emoji", v.getField(emoji).getString());
    assertEquals(emoji, v.fieldNames().get(101));
  }

  @Test
  public void testGetWithTargetType() {
    Variant v =
        Variant.parseJson(
            "{\"id\": 42, \"amount\": 19.99, \"big\": 300, \"ok\": true, \"user\": \"amy\","
                + " \"tags\": [\"a\", \"b\"], \"none\": null}");
    assertEquals(Long.valueOf(42), v.get("$.id", FieldType.INT64));
    assertEquals(Integer.valueOf(42), v.get("$.id", FieldType.INT32));
    assertEquals(Byte.valueOf((byte) 42), v.get("$.id", FieldType.BYTE));
    assertEquals(new BigDecimal("19.99"), v.get("$.amount", FieldType.DECIMAL));
    assertEquals(19.99, (Double) v.get("$.amount", FieldType.DOUBLE), 0.0);
    assertEquals(Boolean.TRUE, v.get("$.ok", FieldType.BOOLEAN));
    assertEquals("amy", v.get("$.user", FieldType.STRING));
    // Non-string values convert to their JSON text.
    assertEquals("42", v.get("$.id", FieldType.STRING));
    assertEquals("[\"a\",\"b\"]", v.get("$.tags", FieldType.STRING));
    // Sub-Variants.
    assertEquals(
        Variant.parseJson("[\"a\",\"b\"]"),
        v.get("$.tags", FieldType.logicalType(SqlTypes.VARIANT)));
    // Missing path and Variant null both give null.
    assertNull(v.get("$.missing", FieldType.INT64));
    assertNull(v.get("$.none", FieldType.INT64));

    // Failed casts: get throws, tryGet returns null.
    assertThrows(IllegalArgumentException.class, () -> v.get("$.user", FieldType.INT64));
    assertThrows(IllegalArgumentException.class, () -> v.get("$.amount", FieldType.INT64));
    assertThrows(IllegalArgumentException.class, () -> v.get("$.big", FieldType.BYTE));
    assertNull(v.tryGet("$.user", FieldType.INT64));
    assertNull(v.tryGet("$.big", FieldType.BYTE));
    assertEquals(Long.valueOf(300), v.tryGet("$.big", FieldType.INT64));
    // A malformed path is not a cast failure.
    assertThrows(IllegalArgumentException.class, () -> v.tryGet("id", FieldType.INT64));
  }

  @Test
  public void testGetTemporalAndUuidTypes() throws Exception {
    assertEquals(
        LocalDate.of(2025, 4, 16),
        vector("primitive_date").get("$", FieldType.logicalType(SqlTypes.DATE)));
    assertEquals(
        Instant.parse("2025-04-16T16:34:56.780Z"),
        vector("primitive_timestamp").get("$", FieldType.logicalType(Timestamp.MICROS)));
    assertEquals(
        LocalDateTime.parse("2025-04-16T12:34:56.780"),
        vector("primitive_timestampntz").get("$", FieldType.logicalType(SqlTypes.DATETIME)));
    assertEquals(
        LocalTime.parse("12:33:54.123456"),
        vector("primitive_time").get("$", FieldType.logicalType(SqlTypes.TIME)));
    assertEquals(
        UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
        vector("primitive_uuid").get("$", FieldType.logicalType(SqlTypes.UUID)));
    assertNull(vector("primitive_date").tryGet("$", FieldType.logicalType(SqlTypes.UUID)));
  }

  @Test
  public void testMalformedInputIsRejected() {
    assertThrows(MalformedVariantException.class, () -> Variant.of(new byte[0], new byte[] {0}));
    // Unsupported version 2.
    assertThrows(
        MalformedVariantException.class, () -> Variant.of(new byte[] {2, 0, 0}, new byte[] {0}));
    // int64 header without payload.
    assertThrows(
        MalformedVariantException.class,
        () -> Variant.of(new byte[] {1, 0, 0}, new byte[] {0x18, 1}));
  }

  @Test
  public void testWrongTypeAccessThrows() {
    Variant s = Variant.parseJson("\"text\"");
    assertThrows(IllegalStateException.class, s::getLong);
    assertThrows(IllegalStateException.class, s::objectSize);
    assertThrows(IllegalStateException.class, () -> s.getElement(0));
  }
}
