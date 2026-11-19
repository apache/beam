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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;

/**
 * The official Parquet Variant conformance vectors from <a
 * href="https://github.com/apache/parquet-testing/tree/master/variant">apache/parquet-testing</a>
 * ({@code <name>.metadata} / {@code <name>.value}), embedded as hex so no binary test resources are
 * needed.
 */
final class ParquetTestingVectors {
  private ParquetTestingVectors() {}

  private static final Map<String, String[]> VECTORS = new LinkedHashMap<>();

  static {
    VECTORS.put("array_empty", new String[] {"010000", "030000"});
    VECTORS.put(
        "array_nested",
        new String[] {
          "01040002070c1069647468696e676e616d657374797065",
          "0303002526450202000100021e0c0102010200170302000b1229436f6e7472617269616e19537069"
              + "646572000203000203000502160c02096966030300060a0b154170706c650d52617900"
        });
    VECTORS.put("array_primitive", new String[] {"010000", "030400020406080c020c010c050c09"});
    VECTORS.put(
        "long_string",
        new String[] {
          "010000",
          "40980000005468697320737472696e6720697320666f72207375726520616e64206365727461696e"
              + "6c79206c6f6e676572207468616e20363420627974657320616e6420697420616c736f20696e636c"
              + "75646573207365766572616c206e6f6e206173636969206368617261637465727320737563682061"
              + "7320f09f90a22c20f09f92962c20e299a5efb88f2c20f09f8ea320616e6420f09fa4a62121"
        });
    VECTORS.put("object_empty", new String[] {"010000", "020000"});
    VECTORS.put(
        "object_nested",
        new String[] {
          "010a0002090d1722262e333e466964737065636965736e616d65706f70756c6174696f6e6f627365"
              + "72766174696f6e74696d656c6f636174696f6e76616c756574656d706572617475726568756d6964"
              + "697479",
          "0203000401001902460c0102020203000d10316c617661206d6f6e7374657210851a020306050709"
              + "0018242131323a33343a353639496e2074686520566f6c63616e6f020209080200050c7b10c801"
        });
    VECTORS.put(
        "object_primitive",
        new String[] {
          "0107000915273a46505f696e745f6669656c64646f75626c655f6669656c64626f6f6c65616e5f74"
              + "7275655f6669656c64626f6f6c65616e5f66616c73655f6669656c64737472696e675f6669656c64"
              + "6e756c6c5f6669656c6474696d657374616d705f6669656c64",
          "02070302010005040609080200190a1a310c01200815cd5b07040839417061636865205061727175"
              + "65740059323032352d30342d31365431323a33343a35362e3738"
        });
    VECTORS.put("primitive_binary", new String[] {"010000", "3c09000000031337deadbeefcafe"});
    VECTORS.put("primitive_boolean_false", new String[] {"010000", "08"});
    VECTORS.put("primitive_boolean_true", new String[] {"010000", "04"});
    VECTORS.put("primitive_date", new String[] {"010000", "2ce24e0000"});
    VECTORS.put(
        "primitive_decimal16", new String[] {"010000", "2802d2b623c0f41022110000000000000000"});
    VECTORS.put("primitive_decimal4", new String[] {"010000", "2002d2040000"});
    VECTORS.put("primitive_decimal8", new String[] {"010000", "2402d202964900000000"});
    VECTORS.put("primitive_double", new String[] {"010000", "1cc9e587b48065d241"});
    VECTORS.put("primitive_float", new String[] {"010000", "38062c934e"});
    VECTORS.put("primitive_int16", new String[] {"010000", "10d204"});
    VECTORS.put("primitive_int32", new String[] {"010000", "1440e20100"});
    VECTORS.put("primitive_int64", new String[] {"010000", "181581e97df4102211"});
    VECTORS.put("primitive_int8", new String[] {"010000", "0c2a"});
    VECTORS.put("primitive_null", new String[] {"010000", "00"});
    VECTORS.put(
        "primitive_string",
        new String[] {
          "010000",
          "40ae0000005468697320737472696e67206973206c6f6e676572207468616e203634206279746573"
              + "20616e64207468657265666f726520646f6573206e6f742066697420696e20612073686f72745f73"
              + "7472696e6720616e6420697420616c736f20696e636c75646573207365766572616c206e6f6e2061"
              + "736369692063686172616374657273207375636820617320f09f90a22c20f09f92962c20e299a5ef"
              + "b88f2c20f09f8ea320616e6420f09fa4a62121"
        });
    VECTORS.put("primitive_time", new String[] {"010000", "44c0f229880a000000"});
    VECTORS.put("primitive_timestamp", new String[] {"010000", "30e05297dde7320600"});
    VECTORS.put("primitive_timestamp_nanos", new String[] {"010000", "4815413a6cb7af0518"});
    VECTORS.put("primitive_timestampntz", new String[] {"010000", "34e0c24883e4320600"});
    VECTORS.put("primitive_timestampntz_nanos", new String[] {"010000", "4c15413a6cb7af0518"});
    VECTORS.put("primitive_uuid", new String[] {"010000", "50f24f9b6481fa49d1b74e8c09a6e31c56"});
    VECTORS.put(
        "short_string",
        new String[] {
          "010000", "954c657373207468616e2036342062797465732028e29da4efb88f2077697468207574663829"
        });
  }

  static Map<String, String[]> all() {
    return Collections.unmodifiableMap(VECTORS);
  }

  static byte[] metadata(String name) {
    return BaseEncoding.base16().lowerCase().decode(get(name)[0]);
  }

  static byte[] value(String name) {
    return BaseEncoding.base16().lowerCase().decode(get(name)[1]);
  }

  private static String[] get(String name) {
    String[] vector = VECTORS.get(name);
    if (vector == null) {
      throw new IllegalArgumentException("Unknown test vector " + name);
    }
    return vector;
  }
}
