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

import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_ARRAY;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_OBJECT;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_PRIMITIVE;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_SHORT_STRING;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.basicType;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.checkBounds;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.readSigned;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.readUnsigned;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.valueHeader;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.schemas.logicaltypes.VariantLogicalType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A semi-structured value (object, array or primitive) stored in the <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant
 * binary encoding</a>, version 1.
 *
 * <p>A {@code Variant} is a pair of byte arrays, {@link #metadata()} (a dictionary of object keys)
 * and {@link #value()} (the self-describing encoded value). This is the same representation used by
 * Apache Iceberg v3, Apache Spark ({@code VariantVal}) and Apache Flink ({@code BinaryVariant}), so
 * the bytes can be exchanged with those systems as-is.
 *
 * <p>In Beam schemas a {@code Variant} is represented by the {@link
 * org.apache.beam.sdk.schemas.logicaltypes.VariantLogicalType} logical type ({@code
 * beam:logical_type:variant:v1}).
 *
 * <h3>Creating values</h3>
 *
 * <pre>{@code
 * Variant v = Variant.parseJson("{\"event\":\"login\",\"device\":{\"os\":\"ios\"}}");
 * Variant raw = Variant.of(metadataBytes, valueBytes); // e.g. bytes read from Iceberg / Spark
 * }</pre>
 *
 * <h3>Reading values</h3>
 *
 * <pre>{@code
 * v.get("$.device.os").getString();   // "ios"
 * v.getField("event").getType();      // VariantType.STRING
 * v.toJson();                         // {"device":{"os":"ios"},"event":"login"}
 *
 * // Extract and convert to a Beam type (Spark's variant_get / try_variant_get).
 * String os = v.get("$.device.os", FieldType.STRING);          // "ios"
 * Long n = v.tryGet("$.device.os", FieldType.INT64);           // null: not a number
 * }</pre>
 *
 * <h3>Equality</h3>
 *
 * <p>{@link #equals} and {@link #hashCode} compare the <em>logical</em> value: for example an INT8
 * {@code 1} equals an INT64 {@code 1} and a DECIMAL {@code 1.0}, and object field order and
 * dictionary layout do not matter. Note that {@link org.apache.beam.sdk.values.Row#equals} compares
 * the encoded bytes of a Variant field, and that a Variant must not be used as (part of) a grouping
 * key, since logically equal values can have different encodings.
 *
 * <p>Instances are immutable as long as the arrays passed to {@link #of} are not modified.
 */
public final class Variant implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  /** Objects with at most this many fields are searched linearly. */
  private static final int LINEAR_SEARCH_THRESHOLD = 32;

  private final byte[] metadata;
  private final byte[] valueBuffer;

  /** Start of this value inside {@link #valueBuffer}. Non-zero for nested values. */
  private final int pos;

  private Variant(byte[] metadata, byte[] valueBuffer, int pos) {
    this.metadata = metadata;
    this.valueBuffer = valueBuffer;
    this.pos = pos;
  }

  // ---------------------------------------------------------------------------------------------
  // Factories
  // ---------------------------------------------------------------------------------------------

  /**
   * Wraps existing Variant-encoded bytes without copying them. Throws {@link
   * MalformedVariantException} if the header bytes are invalid.
   */
  public static Variant of(byte[] metadata, byte[] value) {
    Objects.requireNonNull(metadata, "metadata");
    Objects.requireNonNull(value, "value");
    if (metadata.length < 1) {
      throw new MalformedVariantException("Variant metadata must not be empty");
    }
    int version = metadata[0] & VariantEncoding.VERSION_MASK;
    if (version != VariantEncoding.VERSION) {
      throw new MalformedVariantException("Unsupported Variant metadata version " + version);
    }
    if (value.length < 1) {
      throw new MalformedVariantException("Variant value must not be empty");
    }
    Variant variant = new Variant(metadata, value, 0);
    int size = variant.valueSize();
    if (size > value.length) {
      throw new MalformedVariantException(
          String.format(
              "Variant value is truncated: header requires %d bytes, got %d", size, value.length));
    }
    return variant;
  }

  /**
   * Parses JSON text into a Variant, following Apache Spark's {@code parse_json} rules.
   *
   * <ul>
   *   <li>Integers use the smallest integer type that fits.
   *   <li>Numbers with a fraction (and no exponent) become the smallest exact decimal, falling back
   *       to DOUBLE if more than 38 digits are needed. Numbers with an exponent become DOUBLE.
   *   <li>Duplicate object keys are an error.
   *   <li>Strings stay strings; no timestamp detection is done.
   * </ul>
   *
   * @throws IllegalArgumentException if {@code json} is not valid JSON.
   */
  public static Variant parseJson(String json) {
    return VariantBuilder.parseJson(json);
  }

  /** Like {@link #parseJson} but returns {@code null} on invalid input. */
  public static @Nullable Variant tryParseJson(@Nullable String json) {
    if (json == null) {
      return null;
    }
    try {
      return VariantBuilder.parseJson(json);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Raw bytes
  // ---------------------------------------------------------------------------------------------

  /** The metadata (key dictionary) bytes. Zero-copy: the returned array must not be modified. */
  public byte[] metadata() {
    return metadata;
  }

  /**
   * The encoded value bytes. For a top-level Variant this is the array passed to {@link #of}
   * (zero-copy); for a nested value obtained via {@link #getField} etc. it is a copy of the value's
   * slice. The returned array must not be modified.
   */
  public byte[] value() {
    int size = valueSize();
    if (pos == 0 && size == valueBuffer.length) {
      return valueBuffer;
    }
    return Arrays.copyOfRange(valueBuffer, pos, pos + size);
  }

  // ---------------------------------------------------------------------------------------------
  // Type inspection
  // ---------------------------------------------------------------------------------------------

  private int header() {
    checkBounds(valueBuffer, pos, 1);
    return valueBuffer[pos] & 0xFF;
  }

  /** Returns the type of this value. */
  public VariantType getType() {
    int header = header();
    switch (basicType(header)) {
      case BASIC_TYPE_SHORT_STRING:
        return VariantType.STRING;
      case BASIC_TYPE_OBJECT:
        return VariantType.OBJECT;
      case BASIC_TYPE_ARRAY:
        return VariantType.ARRAY;
      default:
        return primitiveVariantType(valueHeader(header));
    }
  }

  private static VariantType primitiveVariantType(int primitiveType) {
    switch (primitiveType) {
      case VariantEncoding.PRIMITIVE_NULL:
        return VariantType.NULL;
      case VariantEncoding.PRIMITIVE_TRUE:
      case VariantEncoding.PRIMITIVE_FALSE:
        return VariantType.BOOLEAN;
      case VariantEncoding.PRIMITIVE_INT8:
        return VariantType.INT8;
      case VariantEncoding.PRIMITIVE_INT16:
        return VariantType.INT16;
      case VariantEncoding.PRIMITIVE_INT32:
        return VariantType.INT32;
      case VariantEncoding.PRIMITIVE_INT64:
        return VariantType.INT64;
      case VariantEncoding.PRIMITIVE_DOUBLE:
        return VariantType.DOUBLE;
      case VariantEncoding.PRIMITIVE_DECIMAL4:
        return VariantType.DECIMAL4;
      case VariantEncoding.PRIMITIVE_DECIMAL8:
        return VariantType.DECIMAL8;
      case VariantEncoding.PRIMITIVE_DECIMAL16:
        return VariantType.DECIMAL16;
      case VariantEncoding.PRIMITIVE_DATE:
        return VariantType.DATE;
      case VariantEncoding.PRIMITIVE_TIMESTAMP:
        return VariantType.TIMESTAMP;
      case VariantEncoding.PRIMITIVE_TIMESTAMP_NTZ:
        return VariantType.TIMESTAMP_NTZ;
      case VariantEncoding.PRIMITIVE_FLOAT:
        return VariantType.FLOAT;
      case VariantEncoding.PRIMITIVE_BINARY:
        return VariantType.BINARY;
      case VariantEncoding.PRIMITIVE_STRING:
        return VariantType.STRING;
      case VariantEncoding.PRIMITIVE_TIME:
        return VariantType.TIME;
      case VariantEncoding.PRIMITIVE_TIMESTAMP_NANOS:
        return VariantType.TIMESTAMP_NANOS;
      case VariantEncoding.PRIMITIVE_TIMESTAMP_NTZ_NANOS:
        return VariantType.TIMESTAMP_NTZ_NANOS;
      case VariantEncoding.PRIMITIVE_UUID:
        return VariantType.UUID;
      default:
        throw new MalformedVariantException("Unknown Variant primitive type " + primitiveType);
    }
  }

  /** Whether this is Variant null (JSON null). Distinct from a SQL/Java null field. */
  public boolean isVariantNull() {
    return getType() == VariantType.NULL;
  }

  /** Total encoded size of this value in bytes. */
  int valueSize() {
    int header = header();
    int vh = valueHeader(header);
    switch (basicType(header)) {
      case BASIC_TYPE_SHORT_STRING:
        return 1 + vh;
      case BASIC_TYPE_OBJECT:
        {
          ContainerLayout layout = objectLayout();
          return layout.dataStart - pos + layout.offset(layout.numElements);
        }
      case BASIC_TYPE_ARRAY:
        {
          ContainerLayout layout = arrayLayout();
          return layout.dataStart - pos + layout.offset(layout.numElements);
        }
      default:
        switch (vh) {
          case VariantEncoding.PRIMITIVE_NULL:
          case VariantEncoding.PRIMITIVE_TRUE:
          case VariantEncoding.PRIMITIVE_FALSE:
            return 1;
          case VariantEncoding.PRIMITIVE_INT8:
            return 2;
          case VariantEncoding.PRIMITIVE_INT16:
            return 3;
          case VariantEncoding.PRIMITIVE_INT32:
          case VariantEncoding.PRIMITIVE_DATE:
          case VariantEncoding.PRIMITIVE_FLOAT:
            return 5;
          case VariantEncoding.PRIMITIVE_INT64:
          case VariantEncoding.PRIMITIVE_DOUBLE:
          case VariantEncoding.PRIMITIVE_TIMESTAMP:
          case VariantEncoding.PRIMITIVE_TIMESTAMP_NTZ:
          case VariantEncoding.PRIMITIVE_TIME:
          case VariantEncoding.PRIMITIVE_TIMESTAMP_NANOS:
          case VariantEncoding.PRIMITIVE_TIMESTAMP_NTZ_NANOS:
            return 9;
          case VariantEncoding.PRIMITIVE_DECIMAL4:
            return 6;
          case VariantEncoding.PRIMITIVE_DECIMAL8:
            return 10;
          case VariantEncoding.PRIMITIVE_DECIMAL16:
            return 18;
          case VariantEncoding.PRIMITIVE_UUID:
            return 17;
          case VariantEncoding.PRIMITIVE_BINARY:
          case VariantEncoding.PRIMITIVE_STRING:
            return 5 + readUnsigned(valueBuffer, pos + 1, 4);
          default:
            throw new MalformedVariantException("Unknown Variant primitive type " + vh);
        }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Primitive accessors
  // ---------------------------------------------------------------------------------------------

  private IllegalStateException wrongType(String expected) {
    return new IllegalStateException(
        String.format("Expected a Variant %s but found %s", expected, getType()));
  }

  public boolean getBoolean() {
    int header = header();
    if (basicType(header) == BASIC_TYPE_PRIMITIVE) {
      int type = valueHeader(header);
      if (type == VariantEncoding.PRIMITIVE_TRUE) {
        return true;
      } else if (type == VariantEncoding.PRIMITIVE_FALSE) {
        return false;
      }
    }
    throw wrongType("BOOLEAN");
  }

  /** Returns an INT8, INT16, INT32 or INT64 value as a {@code long}. */
  public long getLong() {
    switch (getType()) {
      case INT8:
        return readSigned(valueBuffer, pos + 1, 1);
      case INT16:
        return readSigned(valueBuffer, pos + 1, 2);
      case INT32:
        return readSigned(valueBuffer, pos + 1, 4);
      case INT64:
        return readSigned(valueBuffer, pos + 1, 8);
      default:
        throw wrongType("integer");
    }
  }

  /** Returns a DOUBLE or FLOAT value as a {@code double}. */
  public double getDouble() {
    switch (getType()) {
      case DOUBLE:
        return Double.longBitsToDouble(readSigned(valueBuffer, pos + 1, 8));
      case FLOAT:
        return getFloat();
      default:
        throw wrongType("DOUBLE");
    }
  }

  public float getFloat() {
    if (getType() != VariantType.FLOAT) {
      throw wrongType("FLOAT");
    }
    return Float.intBitsToFloat((int) readSigned(valueBuffer, pos + 1, 4));
  }

  /** Returns a DECIMAL4/8/16 value, or an integer value converted to a decimal. */
  public BigDecimal getDecimal() {
    VariantType type = getType();
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return BigDecimal.valueOf(getLong());
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        {
          checkBounds(valueBuffer, pos + 1, 1);
          int scale = valueBuffer[pos + 1] & 0xFF;
          if (type == VariantType.DECIMAL4) {
            return BigDecimal.valueOf(readSigned(valueBuffer, pos + 2, 4), scale);
          } else if (type == VariantType.DECIMAL8) {
            return BigDecimal.valueOf(readSigned(valueBuffer, pos + 2, 8), scale);
          }
          checkBounds(valueBuffer, pos + 2, 16);
          byte[] bigEndian = new byte[16];
          for (int i = 0; i < 16; i++) {
            bigEndian[i] = valueBuffer[pos + 2 + 15 - i];
          }
          return new BigDecimal(new BigInteger(bigEndian), scale);
        }
      default:
        throw wrongType("DECIMAL");
    }
  }

  public String getString() {
    int header = header();
    if (basicType(header) == BASIC_TYPE_SHORT_STRING) {
      int length = valueHeader(header);
      checkBounds(valueBuffer, pos + 1, length);
      return new String(valueBuffer, pos + 1, length, StandardCharsets.UTF_8);
    }
    if (basicType(header) == BASIC_TYPE_PRIMITIVE
        && valueHeader(header) == VariantEncoding.PRIMITIVE_STRING) {
      int length = readUnsigned(valueBuffer, pos + 1, 4);
      checkBounds(valueBuffer, pos + 5, length);
      return new String(valueBuffer, pos + 5, length, StandardCharsets.UTF_8);
    }
    throw wrongType("STRING");
  }

  public byte[] getBinary() {
    if (getType() != VariantType.BINARY) {
      throw wrongType("BINARY");
    }
    int length = readUnsigned(valueBuffer, pos + 1, 4);
    checkBounds(valueBuffer, pos + 5, length);
    return Arrays.copyOfRange(valueBuffer, pos + 5, pos + 5 + length);
  }

  public LocalDate getDate() {
    if (getType() != VariantType.DATE) {
      throw wrongType("DATE");
    }
    return LocalDate.ofEpochDay(readSigned(valueBuffer, pos + 1, 4));
  }

  /** Returns a TIMESTAMP or TIMESTAMP_NANOS (UTC adjusted) value. */
  public Instant getTimestamp() {
    VariantType type = getType();
    if (type == VariantType.TIMESTAMP) {
      return microsToInstant(readSigned(valueBuffer, pos + 1, 8));
    } else if (type == VariantType.TIMESTAMP_NANOS) {
      return nanosToInstant(readSigned(valueBuffer, pos + 1, 8));
    }
    throw wrongType("TIMESTAMP");
  }

  /** Returns a TIMESTAMP_NTZ or TIMESTAMP_NTZ_NANOS value. */
  public LocalDateTime getTimestampNtz() {
    VariantType type = getType();
    Instant instant;
    if (type == VariantType.TIMESTAMP_NTZ) {
      instant = microsToInstant(readSigned(valueBuffer, pos + 1, 8));
    } else if (type == VariantType.TIMESTAMP_NTZ_NANOS) {
      instant = nanosToInstant(readSigned(valueBuffer, pos + 1, 8));
    } else {
      throw wrongType("TIMESTAMP_NTZ");
    }
    return LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), ZoneOffset.UTC);
  }

  public LocalTime getTime() {
    if (getType() != VariantType.TIME) {
      throw wrongType("TIME");
    }
    return LocalTime.ofNanoOfDay(readSigned(valueBuffer, pos + 1, 8) * 1000L);
  }

  public UUID getUuid() {
    if (getType() != VariantType.UUID) {
      throw wrongType("UUID");
    }
    checkBounds(valueBuffer, pos + 1, 16);
    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (valueBuffer[pos + 1 + i] & 0xFF);
      lsb = (lsb << 8) | (valueBuffer[pos + 9 + i] & 0xFF);
    }
    return new UUID(msb, lsb);
  }

  private static Instant microsToInstant(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
  }

  private static Instant nanosToInstant(long nanos) {
    return Instant.ofEpochSecond(
        Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
  }

  // ---------------------------------------------------------------------------------------------
  // Objects and arrays
  // ---------------------------------------------------------------------------------------------

  /** Parsed header of an object or array. */
  private final class ContainerLayout {
    final int numElements;
    final int idStart; // objects only
    final int idSize; // objects only
    final int offsetStart;
    final int offsetSize;
    final int dataStart;

    ContainerLayout(int numElements, int idStart, int idSize, int offsetStart, int offsetSize) {
      this.numElements = numElements;
      this.idStart = idStart;
      this.idSize = idSize;
      this.offsetStart = offsetStart;
      this.offsetSize = offsetSize;
      this.dataStart = offsetStart + (numElements + 1) * offsetSize;
    }

    int offset(int i) {
      return readUnsigned(valueBuffer, offsetStart + i * offsetSize, offsetSize);
    }

    int fieldId(int i) {
      return readUnsigned(valueBuffer, idStart + i * idSize, idSize);
    }

    Variant child(int i) {
      return new Variant(metadata, valueBuffer, dataStart + offset(i));
    }
  }

  private ContainerLayout objectLayout() {
    int vh = valueHeader(header());
    int offsetSize = (vh & 0x03) + 1;
    int idSize = ((vh >> 2) & 0x03) + 1;
    boolean isLarge = ((vh >> 4) & 0x01) == 1;
    int numSize = isLarge ? 4 : 1;
    int numElements = readUnsigned(valueBuffer, pos + 1, numSize);
    int idStart = pos + 1 + numSize;
    return new ContainerLayout(
        numElements, idStart, idSize, idStart + numElements * idSize, offsetSize);
  }

  private ContainerLayout arrayLayout() {
    int vh = valueHeader(header());
    int offsetSize = (vh & 0x03) + 1;
    boolean isLarge = ((vh >> 2) & 0x01) == 1;
    int numSize = isLarge ? 4 : 1;
    int numElements = readUnsigned(valueBuffer, pos + 1, numSize);
    return new ContainerLayout(numElements, 0, 0, pos + 1 + numSize, offsetSize);
  }

  private ContainerLayout requireObject() {
    if (basicType(header()) != BASIC_TYPE_OBJECT) {
      throw wrongType("OBJECT");
    }
    return objectLayout();
  }

  private ContainerLayout requireArray() {
    if (basicType(header()) != BASIC_TYPE_ARRAY) {
      throw wrongType("ARRAY");
    }
    return arrayLayout();
  }

  /** Number of fields of an OBJECT. */
  public int objectSize() {
    return requireObject().numElements;
  }

  /** Field names of an OBJECT, in encoded (lexicographic) order. */
  public List<String> fieldNames() {
    ContainerLayout layout = requireObject();
    List<String> names = new ArrayList<>(layout.numElements);
    for (int i = 0; i < layout.numElements; i++) {
      names.add(metadataKey(layout.fieldId(i)));
    }
    return Collections.unmodifiableList(names);
  }

  /** Returns the value of field {@code name} of an OBJECT, or {@code null} if it is missing. */
  public @Nullable Variant getField(String name) {
    ContainerLayout layout = requireObject();
    int n = layout.numElements;
    if (n > LINEAR_SEARCH_THRESHOLD) {
      // Field ids are sorted by field name (unsigned UTF-8 byte order), so binary search.
      int lo = 0;
      int hi = n - 1;
      while (lo <= hi) {
        int mid = (lo + hi) >>> 1;
        int cmp = VariantEncoding.compareKeys(metadataKey(layout.fieldId(mid)), name);
        if (cmp == 0) {
          return layout.child(mid);
        } else if (cmp < 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      return null;
    }
    for (int i = 0; i < n; i++) {
      if (metadataKey(layout.fieldId(i)).equals(name)) {
        return layout.child(i);
      }
    }
    return null;
  }

  /** Number of elements of an ARRAY. */
  public int arraySize() {
    return requireArray().numElements;
  }

  /** Returns element {@code index} of an ARRAY. */
  public Variant getElement(int index) {
    ContainerLayout layout = requireArray();
    if (index < 0 || index >= layout.numElements) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Index %d out of bounds for Variant array of size %d", index, layout.numElements));
    }
    return layout.child(index);
  }

  private String metadataKey(int id) {
    int header = metadata[0] & 0xFF;
    int offsetSize = ((header >> 6) & 0x03) + 1;
    int dictionarySize = readUnsigned(metadata, 1, offsetSize);
    if (id < 0 || id >= dictionarySize) {
      throw new MalformedVariantException(
          String.format("Field id %d out of range for dictionary of size %d", id, dictionarySize));
    }
    int offsetStart = 1 + offsetSize;
    int stringStart = offsetStart + (dictionarySize + 1) * offsetSize;
    int start = readUnsigned(metadata, offsetStart + id * offsetSize, offsetSize);
    int end = readUnsigned(metadata, offsetStart + (id + 1) * offsetSize, offsetSize);
    if (end < start) {
      throw new MalformedVariantException("Metadata offsets are not monotonic");
    }
    checkBounds(metadata, stringStart + start, end - start);
    return new String(metadata, stringStart + start, end - start, StandardCharsets.UTF_8);
  }

  // ---------------------------------------------------------------------------------------------
  // Paths
  // ---------------------------------------------------------------------------------------------

  /**
   * Extracts the value at a JSON path, using the same syntax as Apache Spark's {@code variant_get}:
   * {@code $} is the root, followed by any number of {@code .name}, {@code ['name']} or {@code
   * [index]} segments, e.g. {@code $.device.os} or {@code $.items[0]['sku.id']}.
   *
   * @return the value, or {@code null} if the path does not exist (missing key, index out of range,
   *     or indexing into a value of the wrong type).
   * @throws IllegalArgumentException if the path is malformed.
   */
  public @Nullable Variant get(String path) {
    Variant current = this;
    for (Object segment : VariantPath.parse(path)) {
      if (segment instanceof String) {
        if (current.getType() != VariantType.OBJECT) {
          return null;
        }
        current = current.getField((String) segment);
      } else {
        int index = (Integer) segment;
        if (current.getType() != VariantType.ARRAY || index >= current.arraySize()) {
          return null;
        }
        current = current.getElement(index);
      }
      if (current == null) {
        return null;
      }
    }
    return current;
  }

  /**
   * Extracts the value at {@code path} and converts it to a Beam type, like Spark's {@code
   * variant_get(v, path, type)}.
   *
   * <p>Supported target types: {@code BOOLEAN}, {@code BYTE}, {@code INT16}, {@code INT32}, {@code
   * INT64}, {@code FLOAT}, {@code DOUBLE}, {@code DECIMAL}, {@code STRING}, {@code BYTES}, {@code
   * DATETIME}, and the logical types {@code SqlTypes.DATE}, {@code SqlTypes.TIME}, {@code
   * SqlTypes.DATETIME}, {@code Timestamp}, {@code MicrosInstant}, {@code NanosInstant}, {@code
   * SqlTypes.UUID}, {@code FixedPrecisionNumeric} and {@code SqlTypes.VARIANT}. Integer targets
   * accept any exact numeric that is integral and in range. {@code STRING} accepts any value;
   * non-strings are returned as JSON text.
   *
   * @return the converted value (the logical type's input type for logical types), or {@code null}
   *     if the path doesn't exist or holds Variant null.
   * @throws IllegalArgumentException if the path is malformed or the value can't be converted.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> @Nullable T get(String path, Schema.FieldType type) {
    Variant value = get(path);
    if (value == null || value.isVariantNull()) {
      return null;
    }
    try {
      @SuppressWarnings("unchecked")
      T result = (T) value.convertTo(type);
      return result;
    } catch (IllegalStateException | ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format("Cannot convert Variant %s at %s to %s", value.getType(), path, type), e);
    }
  }

  /**
   * Like {@link #get(String, Schema.FieldType)}, but returns {@code null} instead of throwing when
   * the value can't be converted, like Spark's {@code try_variant_get}. A malformed path still
   * throws.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> @Nullable T tryGet(String path, Schema.FieldType type) {
    VariantPath.parse(path); // validate the path; malformed paths are not cast failures
    try {
      return get(path, type);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private Object convertTo(Schema.FieldType type) {
    switch (type.getTypeName()) {
      case BOOLEAN:
        return getBoolean();
      case BYTE:
        return exactInteger().byteValueExact();
      case INT16:
        return exactInteger().shortValueExact();
      case INT32:
        return exactInteger().intValueExact();
      case INT64:
        return exactInteger().longValueExact();
      case FLOAT:
        return (float) numericAsDouble();
      case DOUBLE:
        return numericAsDouble();
      case DECIMAL:
        return numericAsDecimal();
      case STRING:
        return getType() == VariantType.STRING ? getString() : toJson();
      case BYTES:
        return getBinary();
      case DATETIME:
        return new org.joda.time.Instant(getTimestamp().toEpochMilli());
      case LOGICAL_TYPE:
        return convertToLogicalType(
            checkArgumentNotNull(type.getLogicalType()).getIdentifier(), type);
      default:
        throw new UnsupportedOperationException(
            "Converting a Variant to "
                + type
                + " is not supported; extract it as SqlTypes.VARIANT instead");
    }
  }

  private Object convertToLogicalType(String identifier, Schema.FieldType type) {
    if (VariantLogicalType.IDENTIFIER.equals(identifier)) {
      return this;
    } else if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
      return getDate();
    } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
      return getTime();
    } else if (SqlTypes.DATETIME.getIdentifier().equals(identifier)) {
      return getTimestampNtz();
    } else if (Timestamp.IDENTIFIER.equals(identifier)
        || MicrosInstant.IDENTIFIER.equals(identifier)
        || NanosInstant.IDENTIFIER.equals(identifier)) {
      return getTimestamp();
    } else if (SqlTypes.UUID.getIdentifier().equals(identifier)) {
      return getUuid();
    } else if (FixedPrecisionNumeric.IDENTIFIER.equals(identifier)) {
      return numericAsDecimal();
    }
    throw new UnsupportedOperationException(
        "Converting a Variant to " + type + " is not supported");
  }

  /** Any exact numeric (integer or decimal) with no fractional part. */
  private BigInteger exactInteger() {
    return numericAsDecimal().toBigIntegerExact();
  }

  private BigDecimal numericAsDecimal() {
    VariantType type = getType();
    if (type == VariantType.FLOAT || type == VariantType.DOUBLE) {
      return BigDecimal.valueOf(getDouble());
    }
    return getDecimal(); // integers and decimals; throws for other types
  }

  private double numericAsDouble() {
    VariantType type = getType();
    if (type == VariantType.FLOAT || type == VariantType.DOUBLE) {
      return getDouble();
    }
    return getDecimal().doubleValue();
  }

  // ---------------------------------------------------------------------------------------------
  // Conversions
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns the JSON representation of this value. Object keys are written in encoded
   * (lexicographic) order. Types JSON does not have are written as strings: dates, times, UUIDs and
   * timestamps in ISO-8601 format (UTC adjusted timestamps end in {@code Z}), binary as base64.
   */
  public String toJson() {
    StringWriter writer = new StringWriter();
    try (JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
      writeJson(gen);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return writer.toString();
  }

  private void writeJson(JsonGenerator gen) throws IOException {
    VariantType type = getType();
    switch (type) {
      case NULL:
        gen.writeNull();
        break;
      case BOOLEAN:
        gen.writeBoolean(getBoolean());
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        gen.writeNumber(getLong());
        break;
      case DOUBLE:
        gen.writeNumber(getDouble());
        break;
      case FLOAT:
        gen.writeNumber(getFloat());
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        gen.writeNumber(getDecimal().toPlainString());
        break;
      case STRING:
        gen.writeString(getString());
        break;
      case BINARY:
        gen.writeString(Base64.getEncoder().encodeToString(getBinary()));
        break;
      case DATE:
        gen.writeString(getDate().toString());
        break;
      case TIMESTAMP:
      case TIMESTAMP_NANOS:
        gen.writeString(getTimestamp().toString());
        break;
      case TIMESTAMP_NTZ:
      case TIMESTAMP_NTZ_NANOS:
        gen.writeString(getTimestampNtz().toString());
        break;
      case TIME:
        gen.writeString(getTime().toString());
        break;
      case UUID:
        gen.writeString(getUuid().toString());
        break;
      case OBJECT:
        {
          ContainerLayout layout = objectLayout();
          gen.writeStartObject();
          for (int i = 0; i < layout.numElements; i++) {
            gen.writeFieldName(metadataKey(layout.fieldId(i)));
            layout.child(i).writeJson(gen);
          }
          gen.writeEndObject();
          break;
        }
      case ARRAY:
        {
          ContainerLayout layout = arrayLayout();
          gen.writeStartArray();
          for (int i = 0; i < layout.numElements; i++) {
            layout.child(i).writeJson(gen);
          }
          gen.writeEndArray();
          break;
        }
      default:
        throw new IllegalStateException("Unhandled Variant type " + type);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Equality
  // ---------------------------------------------------------------------------------------------

  /** Groups of types whose values are compared with each other. */
  private enum Family {
    NULL,
    BOOLEAN,
    EXACT_NUMERIC,
    FLOATING_POINT,
    STRING,
    BINARY,
    DATE,
    TIMESTAMP,
    TIMESTAMP_NTZ,
    TIME,
    UUID,
    OBJECT,
    ARRAY
  }

  private static Family family(VariantType type) {
    switch (type) {
      case NULL:
        return Family.NULL;
      case BOOLEAN:
        return Family.BOOLEAN;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return Family.EXACT_NUMERIC;
      case FLOAT:
      case DOUBLE:
        return Family.FLOATING_POINT;
      case STRING:
        return Family.STRING;
      case BINARY:
        return Family.BINARY;
      case DATE:
        return Family.DATE;
      case TIMESTAMP:
      case TIMESTAMP_NANOS:
        return Family.TIMESTAMP;
      case TIMESTAMP_NTZ:
      case TIMESTAMP_NTZ_NANOS:
        return Family.TIMESTAMP_NTZ;
      case TIME:
        return Family.TIME;
      case UUID:
        return Family.UUID;
      case OBJECT:
        return Family.OBJECT;
      case ARRAY:
        return Family.ARRAY;
      default:
        throw new IllegalStateException("Unhandled Variant type " + type);
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Variant)) {
      return false;
    }
    return logicallyEqual(this, (Variant) o);
  }

  private static boolean logicallyEqual(Variant a, Variant b) {
    Family family = family(a.getType());
    if (family != family(b.getType())) {
      return false;
    }
    switch (family) {
      case NULL:
        return true;
      case BOOLEAN:
        return a.getBoolean() == b.getBoolean();
      case EXACT_NUMERIC:
        return a.getDecimal().compareTo(b.getDecimal()) == 0;
      case FLOATING_POINT:
        return Double.compare(a.getDouble(), b.getDouble()) == 0;
      case STRING:
        return a.getString().equals(b.getString());
      case BINARY:
        return Arrays.equals(a.getBinary(), b.getBinary());
      case DATE:
        return a.getDate().equals(b.getDate());
      case TIMESTAMP:
        return a.getTimestamp().equals(b.getTimestamp());
      case TIMESTAMP_NTZ:
        return a.getTimestampNtz().equals(b.getTimestampNtz());
      case TIME:
        return a.getTime().equals(b.getTime());
      case UUID:
        return a.getUuid().equals(b.getUuid());
      case OBJECT:
        {
          ContainerLayout layout = a.objectLayout();
          if (layout.numElements != b.objectSize()) {
            return false;
          }
          for (int i = 0; i < layout.numElements; i++) {
            Variant other = b.getField(a.metadataKey(layout.fieldId(i)));
            if (other == null || !logicallyEqual(layout.child(i), other)) {
              return false;
            }
          }
          return true;
        }
      case ARRAY:
        {
          int n = a.arraySize();
          if (n != b.arraySize()) {
            return false;
          }
          for (int i = 0; i < n; i++) {
            if (!logicallyEqual(a.getElement(i), b.getElement(i))) {
              return false;
            }
          }
          return true;
        }
      default:
        throw new IllegalStateException("Unhandled family " + family);
    }
  }

  @Override
  public int hashCode() {
    Family family = family(getType());
    switch (family) {
      case NULL:
        return 0;
      case BOOLEAN:
        return Boolean.hashCode(getBoolean());
      case EXACT_NUMERIC:
        {
          BigDecimal d = getDecimal();
          return d.signum() == 0 ? 0 : d.stripTrailingZeros().hashCode();
        }
      case FLOATING_POINT:
        return Double.hashCode(getDouble());
      case STRING:
        return getString().hashCode();
      case BINARY:
        return Arrays.hashCode(getBinary());
      case DATE:
        return getDate().hashCode();
      case TIMESTAMP:
        return getTimestamp().hashCode();
      case TIMESTAMP_NTZ:
        return getTimestampNtz().hashCode();
      case TIME:
        return getTime().hashCode();
      case UUID:
        return getUuid().hashCode();
      case OBJECT:
        {
          // Order independent.
          ContainerLayout layout = objectLayout();
          int h = 17;
          for (int i = 0; i < layout.numElements; i++) {
            h += metadataKey(layout.fieldId(i)).hashCode() ^ layout.child(i).hashCode();
          }
          return h;
        }
      case ARRAY:
        {
          int h = 19;
          int n = arraySize();
          for (int i = 0; i < n; i++) {
            h = 31 * h + getElement(i).hashCode();
          }
          return h;
        }
      default:
        throw new IllegalStateException("Unhandled family " + family);
    }
  }

  /** Returns {@link #toJson()}. */
  @Override
  public String toString() {
    return toJson();
  }
}
