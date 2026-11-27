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

/**
 * Constants and low-level helpers for the <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant
 * binary encoding</a>, version 1. All multi-byte integers are little-endian.
 */
final class VariantEncoding {
  private VariantEncoding() {}

  static final int VERSION = 1;
  static final int VERSION_MASK = 0x0F;
  static final int SORTED_STRINGS_BIT = 0x10;

  // basic_type: the two low bits of the value header byte.
  static final int BASIC_TYPE_PRIMITIVE = 0;
  static final int BASIC_TYPE_SHORT_STRING = 1;
  static final int BASIC_TYPE_OBJECT = 2;
  static final int BASIC_TYPE_ARRAY = 3;

  static final int MAX_SHORT_STRING_LENGTH = 63;

  // Primitive type ids (value_header for basic_type == PRIMITIVE).
  static final int PRIMITIVE_NULL = 0;
  static final int PRIMITIVE_TRUE = 1;
  static final int PRIMITIVE_FALSE = 2;
  static final int PRIMITIVE_INT8 = 3;
  static final int PRIMITIVE_INT16 = 4;
  static final int PRIMITIVE_INT32 = 5;
  static final int PRIMITIVE_INT64 = 6;
  static final int PRIMITIVE_DOUBLE = 7;
  static final int PRIMITIVE_DECIMAL4 = 8;
  static final int PRIMITIVE_DECIMAL8 = 9;
  static final int PRIMITIVE_DECIMAL16 = 10;
  static final int PRIMITIVE_DATE = 11;
  static final int PRIMITIVE_TIMESTAMP = 12;
  static final int PRIMITIVE_TIMESTAMP_NTZ = 13;
  static final int PRIMITIVE_FLOAT = 14;
  static final int PRIMITIVE_BINARY = 15;
  static final int PRIMITIVE_STRING = 16;
  static final int PRIMITIVE_TIME = 17;
  static final int PRIMITIVE_TIMESTAMP_NANOS = 18;
  static final int PRIMITIVE_TIMESTAMP_NTZ_NANOS = 19;
  static final int PRIMITIVE_UUID = 20;

  static final int MAX_DECIMAL4_PRECISION = 9;
  static final int MAX_DECIMAL8_PRECISION = 18;
  static final int MAX_DECIMAL16_PRECISION = 38;

  /**
   * Orders keys the way the spec requires for object fields and sorted dictionaries:
   * lexicographically by unsigned UTF-8 bytes, which is the same as Unicode code point order. This
   * differs from {@link String#compareTo} (UTF-16 code units) for supplementary characters.
   */
  static final java.util.Comparator<String> KEY_ORDER = VariantEncoding::compareKeys;

  static int compareKeys(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      int ca = a.codePointAt(i);
      int cb = b.codePointAt(j);
      if (ca != cb) {
        return Integer.compare(ca, cb);
      }
      i += Character.charCount(ca);
      j += Character.charCount(cb);
    }
    return Integer.compare(a.length() - i, b.length() - j);
  }

  static int basicType(int header) {
    return header & 0x03;
  }

  static int valueHeader(int header) {
    return (header >> 2) & 0x3F;
  }

  static byte primitiveHeader(int primitiveType) {
    // basic_type PRIMITIVE is 0, so the header is just the shifted primitive type id.
    return (byte) (primitiveType << 2);
  }

  /** Reads an unsigned little-endian integer of {@code size} (1-4) bytes. */
  static int readUnsigned(byte[] bytes, int pos, int size) {
    checkBounds(bytes, pos, size);
    int result = 0;
    for (int i = 0; i < size; i++) {
      result |= (bytes[pos + i] & 0xFF) << (8 * i);
    }
    if (result < 0) {
      throw new MalformedVariantException("Offset or size exceeds 2^31-1");
    }
    return result;
  }

  /** Reads a signed little-endian integer of {@code size} (1, 2, 4 or 8) bytes. */
  static long readSigned(byte[] bytes, int pos, int size) {
    checkBounds(bytes, pos, size);
    long result = 0;
    for (int i = 0; i < size; i++) {
      result |= ((long) (bytes[pos + i] & 0xFF)) << (8 * i);
    }
    // Sign extend.
    int shift = 64 - 8 * size;
    return (result << shift) >> shift;
  }

  static void writeLittleEndian(byte[] dest, int pos, long value, int size) {
    for (int i = 0; i < size; i++) {
      dest[pos + i] = (byte) (value >>> (8 * i));
    }
  }

  /** Smallest number of bytes (1-4) that can hold the unsigned value. */
  static int sizeInBytes(int value) {
    if (value < (1 << 8)) {
      return 1;
    } else if (value < (1 << 16)) {
      return 2;
    } else if (value < (1 << 24)) {
      return 3;
    } else {
      return 4;
    }
  }

  static void checkBounds(byte[] bytes, int pos, int size) {
    if (pos < 0 || size < 0 || pos + size > bytes.length) {
      throw new MalformedVariantException(
          String.format(
              "Read of %d bytes at position %d is out of bounds (length %d)",
              size, pos, bytes.length));
    }
  }
}
