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
package org.apache.beam.sdk.coders;

import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.InjectPackageStrategy;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates and caches a fast {@link Coder} for {@link Row} objects matching the {@code
 * beam:coder:row:v1} wire format for a specific {@link Schema}.
 *
 * <p>For schemas with {@code 1 <= fieldCount <= 64}, emits a straight-line unrolled {@link
 * RowFieldCodec} via ByteBuddy so the JVM sees monomorphic field reads/writes with no per-field
 * switch dispatch or branch misprediction. For wider schemas ({@code 65 <= fieldCount <= 1024}),
 * emits chunked 32-field static helper methods on a single generated {@link RowFieldCodec} so every
 * method stays well below HotSpot C2's 8KB {@code HugeMethodLimit} while avoiding switch dispatch.
 * Schemas beyond {@code 1024} fields (or beyond the hard Metaspace budget) execute the shared
 * buffered fallback loop powered by {@link RowWireCodec}.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class RowCoderGenerator {
  private static final Object[] EMPTY_VALUES = new Object[0];
  private static final int MAX_UNROLLED_FIELDS = 64;
  private static final int MAX_CHUNKED_FIELDS = 1024;
  private static final int CHUNK_SIZE = 32;
  // Weighted Metaspace budget: <= 64-field single-method shapes cost 1; wide chunked shapes cost
  // chunkCount (3..32) so wide generated classes cannot silently multiply Metaspace usage.
  private static final int MAX_GENERATED_FIELD_CODEC_WEIGHT = 512;

  private static final String ROW_WIRE_CODEC = "org/apache/beam/sdk/coders/RowWireCodec";
  private static final String SCRATCH_POOL = "org/apache/beam/sdk/coders/RowWireCodec$ScratchPool";
  private static final String SCRATCH_POOL_DESC =
      "Lorg/apache/beam/sdk/coders/RowWireCodec$ScratchPool;";
  private static final String CODER_TYPE = "org/apache/beam/sdk/coders/Coder";
  private static final String ROW_TYPE = "org/apache/beam/sdk/values/Row";
  private static final String OUT_DESC = "Ljava/io/OutputStream;";
  private static final String IN_DESC = "Ljava/io/InputStream;";
  private static final String CODER_DESC = "Lorg/apache/beam/sdk/coders/Coder;";

  // Compact type tags for fast switch fallback and ByteBuddy unrolled field codec dispatch.
  private static final byte KIND_BYTE = 0;
  private static final byte KIND_INT16 = 1;
  private static final byte KIND_INT32 = 2;
  private static final byte KIND_INT64 = 3;
  private static final byte KIND_FLOAT = 4;
  private static final byte KIND_DOUBLE = 5;
  private static final byte KIND_BOOLEAN = 6;
  private static final byte KIND_DATETIME = 7;
  private static final byte KIND_STRING = 8;
  private static final byte KIND_BYTES = 9;
  private static final byte KIND_ROW = 10;
  private static final byte KIND_DECIMAL = 11;
  private static final byte KIND_OTHER = 12;
  private static final byte KIND_DATE = 13;
  private static final byte KIND_TIME = 14;
  private static final byte KIND_LOCAL_DATETIME = 15;
  private static final byte KIND_UUID = 16;
  private static final byte KIND_MICROS_INSTANT = 17;
  private static final byte KIND_NANOS_INSTANT = 18;
  private static final byte KIND_NANOS_DURATION = 19;
  private static final byte KIND_STRING_LIST = 20;
  private static final byte KIND_INT32_LIST = 21;
  private static final byte KIND_INT64_LIST = 22;
  private static final byte KIND_DOUBLE_LIST = 23;
  private static final byte KIND_ROW_LIST = 24;
  private static final byte KIND_STRING_LONG_MAP = 25;
  private static final byte KIND_STRING_STRING_MAP = 26;
  private static final byte KIND_STRING_INT_MAP = 27;
  private static final byte KIND_STRING_DOUBLE_MAP = 28;

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // Bounded non-evicting cache so classes defined into RowCoderGenerator's ClassLoader never
  // thrash Metaspace; shapes beyond MAX_GENERATED_FIELD_CODEC_WEIGHT fall back cleanly to the
  // buffered switch loop.
  private static final ConcurrentHashMap<CodecShapeKey, RowFieldCodec> FIELD_CODEC_CACHE =
      new ConcurrentHashMap<>();
  private static final AtomicInteger GENERATED_FIELD_CODEC_WEIGHT = new AtomicInteger();

  /** Base class for unrolled and chunked stateless per-shape ByteBuddy row field codecs. */
  @Internal
  public abstract static class RowFieldCodec {
    public void encodeArrayNoNulls(
        Object[] values,
        OutputStream out,
        Coder[] coders,
        Coder[] elemCoders,
        boolean staticEncoding)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public void encodeArrayNullable(
        Object[] values, OutputStream out, Coder[] coders, Coder[] elemCoders) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void encodeListNoNulls(
        List<Object> values,
        OutputStream out,
        Coder[] coders,
        Coder[] elemCoders,
        boolean staticEncoding)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public void encodeListNullable(
        List<Object> values, OutputStream out, Coder[] coders, Coder[] elemCoders)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public void encodeRowNoNulls(
        Row row, OutputStream out, Coder[] coders, Coder[] elemCoders, boolean staticEncoding)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public Object[] decodeNoNulls(InputStream in, Coder[] coders, Coder[] elemCoders)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public Object[] decodeNullable(
        InputStream in, Coder[] coders, Coder[] elemCoders, long nullMask, byte[] buf)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public int encodeArrayBody(
        Object[] values, OutputStream out, Coder[] coders, Coder[] elemCoders, byte[] buf, int pos)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public int encodeListBody(
        List<Object> values,
        OutputStream out,
        Coder[] coders,
        Coder[] elemCoders,
        byte[] buf,
        int pos)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public boolean supportsRowBody() {
      return false;
    }

    public boolean supportsNullableWideDecode() {
      return false;
    }

    public int encodeRowBody(
        Row row, OutputStream out, Coder[] coders, Coder[] elemCoders, byte[] buf, int pos)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    public Object[] decodeNoNullsWide(
        InputStream in, Coder[] coders, Coder[] elemCoders, byte[] buf) throws IOException {
      throw new UnsupportedOperationException();
    }

    public Object[] decodeNullableWide(
        InputStream in,
        Coder[] coders,
        Coder[] elemCoders,
        byte[] nullBytes,
        int nullByteLen,
        byte[] buf)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static final class CodecShapeKey {
    final byte[] fieldKinds;
    final int[] encodingPosToIndex;
    final boolean includeNullableDecode;
    final boolean includeRowBody;
    final int hash;

    CodecShapeKey(
        byte[] fieldKinds,
        int[] encodingPosToIndex,
        boolean includeNullableDecode,
        boolean includeRowBody) {
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.includeNullableDecode = includeNullableDecode;
      this.includeRowBody = includeRowBody;
      int h = 31 * Arrays.hashCode(fieldKinds) + Arrays.hashCode(encodingPosToIndex);
      h = 31 * h + (includeNullableDecode ? 1 : 0);
      h = 31 * h + (includeRowBody ? 1 : 0);
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CodecShapeKey)) {
        return false;
      }
      CodecShapeKey that = (CodecShapeKey) o;
      return hash == that.hash
          && includeNullableDecode == that.includeNullableDecode
          && includeRowBody == that.includeRowBody
          && Arrays.equals(fieldKinds, that.fieldKinds)
          && Arrays.equals(encodingPosToIndex, that.encodingPosToIndex);
    }
  }

  static class WithStackTrace<T> {
    private final T value;
    private final String stackTrace;

    public WithStackTrace(T value, String stackTrace) {
      this.value = value;
      this.stackTrace = stackTrace;
    }

    public T getValue() {
      return value;
    }

    public String getStackTrace() {
      return stackTrace;
    }
  }

  // Cache for Coder instances that are already constructed.
  @GuardedBy("cacheLock")
  private static final Map<UUID, WithStackTrace<Coder<Row>>> GENERATED_CODERS = Maps.newHashMap();

  @GuardedBy("cacheLock")
  private static final Map<UUID, WithStackTrace<Map<String, Integer>>> ENCODING_POSITION_OVERRIDES =
      Maps.newHashMap();

  private static final Object cacheLock = new Object();

  private static final Logger LOG = LoggerFactory.getLogger(RowCoderGenerator.class);

  private static String getStackTrace() {
    return StringUtils.arrayToNewlines(Thread.currentThread().getStackTrace(), 10);
  }

  /** Returns whether explicit encoding positions have been registered for the given schema UUID. */
  @Internal
  public static boolean hasEncodingPositionOverride(@Nullable UUID uuid) {
    if (uuid == null) {
      return false;
    }
    synchronized (cacheLock) {
      return ENCODING_POSITION_OVERRIDES.containsKey(uuid);
    }
  }

  /**
   * Resolves the mapping {@code encodingPosition -> rowFieldIndex} for {@code schema}, honoring any
   * registered {@link #overrideEncodingPositions} entry for {@code schema.getUUID()}.
   */
  @Internal
  public static int[] resolveEncodingPosToRowIndex(Schema schema) {
    UUID uuid = Preconditions.checkNotNull(schema.getUUID());
    synchronized (cacheLock) {
      return resolveEncodingPosToRowIndexLocked(schema, uuid);
    }
  }

  @GuardedBy("cacheLock")
  private static int[] resolveEncodingPosToRowIndexLocked(Schema schema, UUID uuid) {
    int fieldCount = schema.getFieldCount();
    int[] encodingPosToRowIndex = new int[fieldCount];
    @Nullable
    WithStackTrace<Map<String, Integer>> existingEncodingPositions =
        ENCODING_POSITION_OVERRIDES.get(uuid);
    Map<String, Integer> encodingPositions =
        existingEncodingPositions == null
            ? schema.getEncodingPositions()
            : existingEncodingPositions.getValue();
    boolean identityPositions = true;
    for (int recordIndex = 0; recordIndex < fieldCount; ++recordIndex) {
      String name = schema.getField(recordIndex).getName();
      Integer pos = encodingPositions.get(name);
      Preconditions.checkState(
          pos != null, "Missing encoding position for field %s in schema %s", name, uuid);
      int encodingPosition = pos;
      encodingPosToRowIndex[encodingPosition] = recordIndex;
      if (encodingPosition != recordIndex) {
        identityPositions = false;
      }
    }
    if (!identityPositions) {
      Preconditions.checkState(
          fieldCount == Arrays.stream(encodingPosToRowIndex).distinct().count());
    }
    return encodingPosToRowIndex;
  }

  /**
   * Decodes the remainder of a {@code beam:coder:row:v1} stream after {@code streamFieldCount} has
   * already been consumed. Used by direct schema coders on schema-evolution fallback paths.
   */
  @Internal
  public static Row decodeAfterFieldCount(
      Coder<Row> delegateCoder, int streamFieldCount, InputStream inStream) throws IOException {
    if (!(delegateCoder instanceof GeneratedRowCoder)) {
      throw new CoderException("Unexpected delegate row coder type: " + delegateCoder);
    }
    int nullByteLen = RowWireCodec.readVarInt(inStream);
    return ((GeneratedRowCoder) delegateCoder)
        .decodeAfterNullByteLen(streamFieldCount, nullByteLen, inStream);
  }

  /**
   * Decodes the remainder of a {@code beam:coder:row:v1} stream after both {@code streamFieldCount}
   * and {@code nullByteLen} have already been consumed.
   */
  @Internal
  public static Row decodeAfterNullByteLen(
      Coder<Row> delegateCoder, int streamFieldCount, int nullByteLen, InputStream inStream)
      throws IOException {
    if (!(delegateCoder instanceof GeneratedRowCoder)) {
      throw new CoderException("Unexpected delegate row coder type: " + delegateCoder);
    }
    return ((GeneratedRowCoder) delegateCoder)
        .decodeAfterNullByteLen(streamFieldCount, nullByteLen, inStream);
  }

  public static void overrideEncodingPositions(UUID uuid, Map<String, Integer> encodingPositions) {
    final String stackTrace = getStackTrace();
    synchronized (cacheLock) {
      @Nullable
      WithStackTrace<Map<String, Integer>> previousEncodingPositions =
          ENCODING_POSITION_OVERRIDES.put(
              uuid, new WithStackTrace<>(encodingPositions, stackTrace));
      @Nullable WithStackTrace<Coder<Row>> existingCoder = GENERATED_CODERS.get(uuid);
      if (previousEncodingPositions == null) {
        if (existingCoder != null) {
          LOG.error(
              "Received encoding positions for uuid {} too late after creating RowCoder. Created: {}\n Override: {}",
              uuid,
              existingCoder.getStackTrace(),
              stackTrace);
        } else {
          LOG.info("Received encoding positions {} for uuid {}.", encodingPositions, uuid);
        }
      } else if (!previousEncodingPositions.getValue().equals(encodingPositions)) {
        if (existingCoder == null) {
          LOG.error(
              "Received differing encoding positions for uuid {} before coder creation. Was {} at {}\n Now {} at {}",
              uuid,
              previousEncodingPositions.getValue(),
              encodingPositions,
              previousEncodingPositions.getStackTrace(),
              stackTrace);
        } else {
          LOG.error(
              "Received differing encoding positions for uuid {} after coder creation at {}\n. "
                  + "Was {} at {}\n Now {} at {}\n",
              uuid,
              existingCoder.getStackTrace(),
              previousEncodingPositions.getValue(),
              encodingPositions,
              previousEncodingPositions.getStackTrace(),
              stackTrace);
        }
      }
    }
  }

  @VisibleForTesting
  static void clearRowCoderCache() {
    synchronized (cacheLock) {
      GENERATED_CODERS.clear();
    }
  }

  @GuardedBy("cacheLock")
  private static boolean canUseDirectRowEncodingLocked(@Nullable Schema rowSchema) {
    if (rowSchema == null) {
      return false;
    }
    if (rowSchema.isEncodingPositionsOverridden() || rowSchema.isStaticEncoding()) {
      return false;
    }
    UUID id = rowSchema.getUUID();
    if (id == null) {
      return true;
    }
    return !ENCODING_POSITION_OVERRIDES.containsKey(id);
  }

  public static Coder<Row> generate(Schema schema) {
    UUID uuid = Preconditions.checkNotNull(schema.getUUID());
    int fieldCount;
    int[] encodingPosToRowIndex;
    boolean identityPositions;
    boolean hasNullableFields;
    Coder[] componentCoders;
    Coder[] elemCoders;
    boolean hasRowList;
    byte[] fieldKinds;
    long fixedPayloadSize;

    // Avoid using computeIfAbsent which may cause issues with nested schemas.
    synchronized (cacheLock) {
      @Nullable WithStackTrace<Coder<Row>> existingRowCoder = GENERATED_CODERS.get(uuid);
      if (existingRowCoder != null) {
        return existingRowCoder.getValue();
      }

      fieldCount = schema.getFieldCount();
      encodingPosToRowIndex = resolveEncodingPosToRowIndexLocked(schema, uuid);
      identityPositions = true;
      for (int i = 0; i < fieldCount; i++) {
        if (encodingPosToRowIndex[i] != i) {
          identityPositions = false;
          break;
        }
      }

      hasNullableFields = schema.hasNullableFields();
      componentCoders = new Coder[fieldCount];
      elemCoders = new Coder[fieldCount];
      hasRowList = false;
      fieldKinds = new byte[fieldCount];
      fixedPayloadSize = hasNullableFields ? -1L : 0L;

      for (int i = 0; i < fieldCount; ++i) {
        int rowIndex = encodingPosToRowIndex[i];
        FieldType fieldType = schema.getField(rowIndex).getType();
        byte kind = fieldKindForFieldTypeLocked(fieldType);
        fieldKinds[i] = kind;
        if (kind == KIND_ROW) {
          Schema nestedRowSchema = fieldType.getRowSchema();
          if (nestedRowSchema != null && nestedRowSchema.getUUID() != null) {
            componentCoders[i] = generate(nestedRowSchema);
          } else {
            // Preserve SchemaCoder.of() behavior of cloning & assigning UUIDs to nested schemas.
            componentCoders[i] = SchemaCoder.coderForFieldType(fieldType.withNullable(false));
          }
        } else {
          // Nulls are handled by RowCoderGenerator's null bitmap, so use withNullable(false).
          componentCoders[i] = SchemaCoder.coderForFieldType(fieldType.withNullable(false));
          if (kind == KIND_ROW_LIST) {
            Schema elemRowSchema = fieldType.getCollectionElementType().getRowSchema();
            if (elemRowSchema != null && elemRowSchema.getUUID() != null) {
              elemCoders[i] = generate(elemRowSchema);
              hasRowList = true;
            } else {
              fieldKinds[i] = KIND_OTHER;
            }
          }
        }

        if (fixedPayloadSize >= 0L) {
          int fixed = fixedByteSizeForKind(kind);
          if (fixed >= 0) {
            fixedPayloadSize += fixed;
          } else {
            fixedPayloadSize = -1L;
          }
        }
      }
    }

    // Generate or look up the stateless ByteBuddy RowFieldCodec outside cacheLock so concurrent
    // callers (and cache hits in GENERATED_CODERS) are never blocked during COMPUTE_FRAMES.
    @Nullable RowFieldCodec fieldCodec = null;
    if (fieldCount >= 1 && fieldCount <= MAX_CHUNKED_FIELDS) {
      boolean includeNullableDecode = fieldCount <= MAX_UNROLLED_FIELDS || hasNullableFields;
      boolean includeRowBody =
          fieldCount <= MAX_UNROLLED_FIELDS || !hasNullableFields || schema.isStaticEncoding();
      fieldCodec =
          getOrGenerateFieldCodec(
              fieldKinds, encodingPosToRowIndex, includeNullableDecode, includeRowBody);
    }

    Coder<Row> rowCoder =
        new GeneratedRowCoder(
            schema,
            componentCoders,
            hasRowList ? elemCoders : null,
            fieldKinds,
            encodingPosToRowIndex,
            hasNullableFields,
            identityPositions,
            fixedPayloadSize,
            fieldCodec);

    synchronized (cacheLock) {
      @Nullable WithStackTrace<Coder<Row>> existingRowCoder = GENERATED_CODERS.get(uuid);
      if (existingRowCoder != null) {
        return existingRowCoder.getValue();
      }
      String stackTrace = LOG.isDebugEnabled() ? getStackTrace() : "";
      GENERATED_CODERS.put(uuid, new WithStackTrace<>(rowCoder, stackTrace));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created row coder for uuid {} at {}", uuid, stackTrace);
      }
      return rowCoder;
    }
  }

  @GuardedBy("cacheLock")
  private static byte fieldKindForFieldTypeLocked(FieldType fieldType) {
    TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case BYTE:
        return KIND_BYTE;
      case INT16:
        return KIND_INT16;
      case INT32:
        return KIND_INT32;
      case INT64:
        return KIND_INT64;
      case FLOAT:
        return KIND_FLOAT;
      case DOUBLE:
        return KIND_DOUBLE;
      case BOOLEAN:
        return KIND_BOOLEAN;
      case DATETIME:
        return KIND_DATETIME;
      case STRING:
        return KIND_STRING;
      case BYTES:
        return KIND_BYTES;
      case ROW:
        return KIND_ROW;
      case DECIMAL:
        return KIND_DECIMAL;
      case LOGICAL_TYPE:
        LogicalType<?, ?> logicalType = fieldType.getLogicalType();
        if (logicalType == null) {
          return KIND_OTHER;
        }
        Class<?> clazz = logicalType.getClass();
        if (clazz == Date.class) {
          return KIND_DATE;
        }
        if (clazz == Time.class) {
          return KIND_TIME;
        }
        if (clazz == DateTime.class && canUseDirectRowEncodingLocked(DateTime.DATETIME_SCHEMA)) {
          return KIND_LOCAL_DATETIME;
        }
        if (clazz == UuidLogicalType.class
            && canUseDirectRowEncodingLocked(UuidLogicalType.UUID_SCHEMA)) {
          return KIND_UUID;
        }
        if (clazz == MicrosInstant.class
            && canUseDirectRowEncodingLocked(logicalType.getBaseType().getRowSchema())) {
          return KIND_MICROS_INSTANT;
        }
        if (clazz == NanosInstant.class
            && canUseDirectRowEncodingLocked(logicalType.getBaseType().getRowSchema())) {
          return KIND_NANOS_INSTANT;
        }
        if (clazz == NanosDuration.class
            && canUseDirectRowEncodingLocked(logicalType.getBaseType().getRowSchema())) {
          return KIND_NANOS_DURATION;
        }
        return KIND_OTHER;
      case ARRAY:
      case ITERABLE:
        FieldType elemType = fieldType.getCollectionElementType();
        if (elemType == null || elemType.getNullable()) {
          return KIND_OTHER;
        }
        switch (elemType.getTypeName()) {
          case STRING:
            return KIND_STRING_LIST;
          case INT32:
            return KIND_INT32_LIST;
          case INT64:
            return KIND_INT64_LIST;
          case DOUBLE:
            return KIND_DOUBLE_LIST;
          case ROW:
            return elemType.getRowSchema() != null ? KIND_ROW_LIST : KIND_OTHER;
          default:
            return KIND_OTHER;
        }
      case MAP:
        FieldType keyType = fieldType.getMapKeyType();
        FieldType valueType = fieldType.getMapValueType();
        if (keyType == null
            || valueType == null
            || keyType.getNullable()
            || valueType.getNullable()
            || keyType.getTypeName() != TypeName.STRING) {
          return KIND_OTHER;
        }
        switch (valueType.getTypeName()) {
          case INT64:
            return KIND_STRING_LONG_MAP;
          case STRING:
            return KIND_STRING_STRING_MAP;
          case INT32:
            return KIND_STRING_INT_MAP;
          case DOUBLE:
            return KIND_STRING_DOUBLE_MAP;
          default:
            return KIND_OTHER;
        }
      default:
        return KIND_OTHER;
    }
  }

  private static int fixedByteSizeForKind(byte kind) {
    switch (kind) {
      case KIND_BYTE:
      case KIND_BOOLEAN:
        return 1;
      case KIND_INT16:
        return 2;
      case KIND_FLOAT:
        return 4;
      case KIND_DOUBLE:
      case KIND_DATETIME:
        return 8;
      default:
        return -1;
    }
  }

  private static @Nullable RowFieldCodec getOrGenerateFieldCodec(
      byte[] fieldKinds,
      int[] encodingPosToRowIndex,
      boolean includeNullableDecode,
      boolean includeRowBody) {
    CodecShapeKey key =
        new CodecShapeKey(fieldKinds, encodingPosToRowIndex, includeNullableDecode, includeRowBody);
    RowFieldCodec cached = FIELD_CODEC_CACHE.get(key);
    if (cached != null) {
      return cached;
    }
    int weight =
        fieldKinds.length <= MAX_UNROLLED_FIELDS
            ? 1
            : (fieldKinds.length + CHUNK_SIZE - 1) / CHUNK_SIZE;
    if (GENERATED_FIELD_CODEC_WEIGHT.get() + weight > MAX_GENERATED_FIELD_CODEC_WEIGHT) {
      return null;
    }
    return FIELD_CODEC_CACHE.computeIfAbsent(
        key,
        k -> {
          if (GENERATED_FIELD_CODEC_WEIGHT.addAndGet(weight) > MAX_GENERATED_FIELD_CODEC_WEIGHT) {
            GENERATED_FIELD_CODEC_WEIGHT.addAndGet(-weight);
            return null;
          }
          try {
            return generateFieldCodec(
                k.fieldKinds, k.encodingPosToIndex, k.includeNullableDecode, k.includeRowBody);
          } catch (Exception | LinkageError e) {
            GENERATED_FIELD_CODEC_WEIGHT.addAndGet(-weight);
            LOG.warn("Falling back to interpreted RowFieldCodec due to generation failure", e);
            return null;
          }
        });
  }

  @Internal
  @VisibleForTesting
  public static boolean hasGeneratedFieldCodec(Coder<Row> coder) {
    return (coder instanceof GeneratedRowCoder) && ((GeneratedRowCoder) coder).fieldCodec != null;
  }

  private static RowFieldCodec generateFieldCodec(
      byte[] fieldKinds,
      int[] encodingPosToIndex,
      boolean includeNullableDecode,
      boolean includeRowBody)
      throws Exception {
    if (fieldKinds.length > MAX_UNROLLED_FIELDS) {
      return generateChunkedFieldCodec(
          fieldKinds, encodingPosToIndex, includeNullableDecode, includeRowBody);
    }
    DynamicType.Builder<RowFieldCodec> builder =
        BYTE_BUDDY
            .with(new InjectPackageStrategy(RowCoderGenerator.class))
            .subclass(RowFieldCodec.class)
            .method(ElementMatchers.named("encodeArrayNoNulls"))
            .intercept(
                new EncodeFieldsImpl(fieldKinds, encodingPosToIndex, SourceKind.ARRAY, false))
            .method(ElementMatchers.named("encodeArrayNullable"))
            .intercept(new EncodeFieldsImpl(fieldKinds, encodingPosToIndex, SourceKind.ARRAY, true))
            .method(ElementMatchers.named("encodeListNoNulls"))
            .intercept(new EncodeFieldsImpl(fieldKinds, encodingPosToIndex, SourceKind.LIST, false))
            .method(ElementMatchers.named("encodeListNullable"))
            .intercept(new EncodeFieldsImpl(fieldKinds, encodingPosToIndex, SourceKind.LIST, true))
            .method(ElementMatchers.named("encodeRowNoNulls"))
            .intercept(new EncodeFieldsImpl(fieldKinds, encodingPosToIndex, SourceKind.ROW, false))
            .method(ElementMatchers.named("decodeNoNulls"))
            .intercept(new DecodeImpl(fieldKinds, encodingPosToIndex, false))
            .method(ElementMatchers.named("decodeNullable"))
            .intercept(new DecodeImpl(fieldKinds, encodingPosToIndex, true));

    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .make()
        .load(
            ReflectHelpers.findClassLoader(RowCoderGenerator.class.getClassLoader()),
            getClassLoadingStrategy(RowCoderGenerator.class))
        .getLoaded()
        .getDeclaredConstructor()
        .newInstance();
  }

  private static RowFieldCodec generateChunkedFieldCodec(
      byte[] fieldKinds,
      int[] encodingPosToIndex,
      boolean includeNullableDecode,
      boolean includeRowBody)
      throws Exception {
    int count = fieldKinds.length;
    int chunkCount = (count + CHUNK_SIZE - 1) / CHUNK_SIZE;
    DynamicType.Builder<RowFieldCodec> builder =
        BYTE_BUDDY
            .with(new InjectPackageStrategy(RowCoderGenerator.class))
            .subclass(RowFieldCodec.class);

    for (int c = 0; c < chunkCount; c++) {
      int start = c * CHUNK_SIZE;
      int end = Math.min(start + CHUNK_SIZE, count);
      // Parameter order puts hot locals (src=0, buf=1, pos=2, out=3) in 1-byte opcode slots.
      builder =
          builder
              .defineMethod("encArrC" + c, int.class, Visibility.PRIVATE, Ownership.STATIC)
              .withParameters(
                  Object[].class,
                  byte[].class,
                  int.class,
                  OutputStream.class,
                  Coder[].class,
                  Coder[].class)
              .throwing(IOException.class)
              .intercept(
                  new EncodeChunkImpl(fieldKinds, encodingPosToIndex, start, end, SourceKind.ARRAY))
              .defineMethod("encListC" + c, int.class, Visibility.PRIVATE, Ownership.STATIC)
              .withParameters(
                  List.class,
                  byte[].class,
                  int.class,
                  OutputStream.class,
                  Coder[].class,
                  Coder[].class)
              .throwing(IOException.class)
              .intercept(
                  new EncodeChunkImpl(fieldKinds, encodingPosToIndex, start, end, SourceKind.LIST))
              .defineMethod("decNoNullC" + c, void.class, Visibility.PRIVATE, Ownership.STATIC)
              .withParameters(
                  Object[].class, InputStream.class, Coder[].class, Coder[].class, byte[].class)
              .throwing(IOException.class)
              .intercept(new DecodeChunkImpl(fieldKinds, encodingPosToIndex, start, end, false));
      if (includeRowBody) {
        builder =
            builder
                .defineMethod("encRowC" + c, int.class, Visibility.PRIVATE, Ownership.STATIC)
                .withParameters(
                    Row.class,
                    byte[].class,
                    int.class,
                    OutputStream.class,
                    Coder[].class,
                    Coder[].class)
                .throwing(IOException.class)
                .intercept(
                    new EncodeChunkImpl(
                        fieldKinds, encodingPosToIndex, start, end, SourceKind.ROW));
      }
      if (includeNullableDecode) {
        builder =
            builder
                .defineMethod("decNullC" + c, void.class, Visibility.PRIVATE, Ownership.STATIC)
                .withParameters(
                    Object[].class,
                    InputStream.class,
                    Coder[].class,
                    Coder[].class,
                    byte[].class,
                    int.class)
                .throwing(IOException.class)
                .intercept(new DecodeChunkImpl(fieldKinds, encodingPosToIndex, start, end, true));
      }
    }

    builder =
        builder
            .method(ElementMatchers.named("encodeArrayBody"))
            .intercept(new ChunkedEncodeDispatchImpl(chunkCount, SourceKind.ARRAY))
            .method(ElementMatchers.named("encodeListBody"))
            .intercept(new ChunkedEncodeDispatchImpl(chunkCount, SourceKind.LIST))
            .method(ElementMatchers.named("decodeNoNullsWide"))
            .intercept(new ChunkedDecodeDispatchImpl(count, chunkCount, false));
    if (includeRowBody) {
      builder =
          builder
              .method(ElementMatchers.named("supportsRowBody"))
              .intercept(FixedValue.value(true))
              .method(ElementMatchers.named("encodeRowBody"))
              .intercept(new ChunkedEncodeDispatchImpl(chunkCount, SourceKind.ROW));
    }
    if (includeNullableDecode) {
      builder =
          builder
              .method(ElementMatchers.named("supportsNullableWideDecode"))
              .intercept(FixedValue.value(true))
              .method(ElementMatchers.named("decodeNullableWide"))
              .intercept(new ChunkedDecodeDispatchImpl(count, chunkCount, true));
    }

    return builder
        .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
        .make()
        .load(
            ReflectHelpers.findClassLoader(RowCoderGenerator.class.getClassLoader()),
            getClassLoadingStrategy(RowCoderGenerator.class))
        .getLoaded()
        .getDeclaredConstructor()
        .newInstance();
  }

  private static void pushInt(MethodVisitor mv, int value) {
    if (value >= -1 && value <= 5) {
      mv.visitInsn(Opcodes.ICONST_0 + value);
    } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      mv.visitIntInsn(Opcodes.BIPUSH, value);
    } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      mv.visitIntInsn(Opcodes.SIPUSH, value);
    } else {
      mv.visitLdcInsn(value);
    }
  }

  private static final String PACK_SUFFIX = "[BI" + OUT_DESC + ")I";

  /** Emits a buffered pack call for a non-null field value loaded in {@code valLocal}. */
  private static void emitPackFieldValue(
      MethodVisitor mv,
      byte kind,
      int pos,
      int valLocal,
      int outLocal,
      int codersLocal,
      int elemCodersLocal,
      int bufLocal,
      int posLocal) {
    switch (kind) {
      case KIND_BYTE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Byte");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Byte", "byteValue", "()B", false);
        emitPackCall(mv, "packByte", "(B" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_INT16:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Short");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Short", "shortValue", "()S", false);
        emitPackCall(mv, "packInt16", "(S" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_INT32:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
        emitPackCall(mv, "packVarInt", "(I" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_INT64:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Long");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J", false);
        emitPackCall(mv, "packVarLong", "(J" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_FLOAT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Float");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue", "()F", false);
        emitPackCall(mv, "packFloat", "(F" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Double");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue", "()D", false);
        emitPackCall(mv, "packDouble", "(D" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_BOOLEAN:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Boolean");
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
        emitPackCall(mv, "packBoolean", "(Z" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "org/joda/time/ReadableInstant");
        emitPackCall(
            mv,
            "packDateTime",
            "(Lorg/joda/time/ReadableInstant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/String");
        emitPackCall(
            mv, "packString", "(Ljava/lang/String;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_BYTES:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "[B");
        emitPackCall(mv, "packBytes", "([B" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_DECIMAL:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/math/BigDecimal");
        emitPackCall(
            mv,
            "packDecimal",
            "(Ljava/math/BigDecimal;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_DATE:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/LocalDate");
        emitPackCall(
            mv,
            "packLocalDate",
            "(Ljava/time/LocalDate;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_TIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/LocalTime");
        emitPackCall(
            mv,
            "packLocalTime",
            "(Ljava/time/LocalTime;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_LOCAL_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/LocalDateTime");
        emitPackCall(
            mv,
            "packLocalDateTime",
            "(Ljava/time/LocalDateTime;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_UUID:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/UUID");
        emitPackCall(
            mv, "packUuid", "(Ljava/util/UUID;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
        break;
      case KIND_MICROS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/Instant");
        emitPackCall(
            mv,
            "packMicrosInstant",
            "(Ljava/time/Instant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_NANOS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/Instant");
        emitPackCall(
            mv,
            "packNanosInstant",
            "(Ljava/time/Instant;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_NANOS_DURATION:
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/time/Duration");
        emitPackCall(
            mv,
            "packNanosDuration",
            "(Ljava/time/Duration;" + PACK_SUFFIX,
            bufLocal,
            posLocal,
            outLocal);
        break;
      case KIND_STRING_LIST:
        emitPackIterableCall(
            mv, "packStringIterable", pos, valLocal, bufLocal, posLocal, outLocal, codersLocal);
        break;
      case KIND_INT32_LIST:
        emitPackIterableCall(
            mv, "packInt32Iterable", pos, valLocal, bufLocal, posLocal, outLocal, codersLocal);
        break;
      case KIND_INT64_LIST:
        emitPackIterableCall(
            mv, "packInt64Iterable", pos, valLocal, bufLocal, posLocal, outLocal, codersLocal);
        break;
      case KIND_DOUBLE_LIST:
        emitPackIterableCall(
            mv, "packDoubleIterable", pos, valLocal, bufLocal, posLocal, outLocal, codersLocal);
        break;
      case KIND_ROW_LIST:
        // Flush buffered bytes before calling nested row element coder.
        emitFlush(mv, bufLocal, posLocal, outLocal);
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Iterable");
        mv.visitVarInsn(Opcodes.ALOAD, elemCodersLocal);
        pushInt(mv, pos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, outLocal);
        mv.visitVarInsn(Opcodes.ALOAD, codersLocal);
        pushInt(mv, pos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "writeElementIterable",
            "(Ljava/lang/Iterable;" + CODER_DESC + OUT_DESC + CODER_DESC + ")V",
            false);
        break;
      case KIND_STRING_LONG_MAP:
        emitPackMapCall(mv, "packStringLongMap", valLocal, bufLocal, posLocal, outLocal);
        break;
      case KIND_STRING_STRING_MAP:
        emitPackMapCall(mv, "packStringStringMap", valLocal, bufLocal, posLocal, outLocal);
        break;
      case KIND_STRING_INT_MAP:
        emitPackMapCall(mv, "packStringIntMap", valLocal, bufLocal, posLocal, outLocal);
        break;
      case KIND_STRING_DOUBLE_MAP:
        emitPackMapCall(mv, "packStringDoubleMap", valLocal, bufLocal, posLocal, outLocal);
        break;
      default:
        // Flush buffered bytes before invoking nested/arbitrary Coder.encode.
        emitFlush(mv, bufLocal, posLocal, outLocal);
        mv.visitVarInsn(Opcodes.ALOAD, codersLocal);
        pushInt(mv, pos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, valLocal);
        mv.visitVarInsn(Opcodes.ALOAD, outLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            CODER_TYPE,
            "encode",
            "(Ljava/lang/Object;" + OUT_DESC + ")V",
            false);
        break;
    }
  }

  private static void emitPackCall(
      MethodVisitor mv, String method, String desc, int bufLocal, int posLocal, int outLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
    mv.visitVarInsn(Opcodes.ILOAD, posLocal);
    mv.visitVarInsn(Opcodes.ALOAD, outLocal);
    mv.visitMethodInsn(Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, method, desc, false);
    mv.visitVarInsn(Opcodes.ISTORE, posLocal);
  }

  private static void emitFlush(MethodVisitor mv, int bufLocal, int posLocal, int outLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
    mv.visitVarInsn(Opcodes.ILOAD, posLocal);
    mv.visitVarInsn(Opcodes.ALOAD, outLocal);
    mv.visitMethodInsn(
        Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "flush", "([BI" + OUT_DESC + ")I", false);
    mv.visitVarInsn(Opcodes.ISTORE, posLocal);
  }

  private static void emitPackIterableCall(
      MethodVisitor mv,
      String methodName,
      int pos,
      int valLocal,
      int bufLocal,
      int posLocal,
      int outLocal,
      int codersLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, valLocal);
    mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Iterable");
    mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
    mv.visitVarInsn(Opcodes.ILOAD, posLocal);
    mv.visitVarInsn(Opcodes.ALOAD, outLocal);
    mv.visitVarInsn(Opcodes.ALOAD, codersLocal);
    pushInt(mv, pos);
    mv.visitInsn(Opcodes.AALOAD);
    mv.visitMethodInsn(
        Opcodes.INVOKESTATIC,
        ROW_WIRE_CODEC,
        methodName,
        "(Ljava/lang/Iterable;[BI" + OUT_DESC + CODER_DESC + ")I",
        false);
    mv.visitVarInsn(Opcodes.ISTORE, posLocal);
  }

  private static void emitPackMapCall(
      MethodVisitor mv, String methodName, int valLocal, int bufLocal, int posLocal, int outLocal) {
    mv.visitVarInsn(Opcodes.ALOAD, valLocal);
    mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/Map");
    emitPackCall(mv, methodName, "(Ljava/util/Map;" + PACK_SUFFIX, bufLocal, posLocal, outLocal);
  }

  /**
   * Pushes the decoded boxed/object field value onto the operand stack using hoisted {@code
   * bufLocal}.
   */
  private static void emitReadFieldValue(
      MethodVisitor mv,
      byte kind,
      int pos,
      int inLocal,
      int codersLocal,
      int elemCodersLocal,
      int bufLocal) {
    switch (kind) {
      case KIND_BYTE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readByte", "(" + IN_DESC + ")B", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
        break;
      case KIND_INT16:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readInt16", "(" + IN_DESC + "[B)S", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
        break;
      case KIND_INT32:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readVarInt", "(" + IN_DESC + ")I", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
        break;
      case KIND_INT64:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readVarLong", "(" + IN_DESC + ")J", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        break;
      case KIND_FLOAT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readFloat", "(" + IN_DESC + "[B)F", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
        break;
      case KIND_DOUBLE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readDouble", "(" + IN_DESC + "[B)D", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
        break;
      case KIND_BOOLEAN:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readBoolean", "(" + IN_DESC + ")Z", false);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
        break;
      case KIND_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readDateTime",
            "(" + IN_DESC + "[B)Lorg/joda/time/Instant;",
            false);
        break;
      case KIND_STRING:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readString",
            "(" + IN_DESC + "[B)Ljava/lang/String;",
            false);
        break;
      case KIND_BYTES:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "readBytes", "(" + IN_DESC + ")[B", false);
        break;
      case KIND_DECIMAL:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readDecimal",
            "(" + IN_DESC + "[B)Ljava/math/BigDecimal;",
            false);
        break;
      case KIND_DATE:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readLocalDate",
            "(" + IN_DESC + ")Ljava/time/LocalDate;",
            false);
        break;
      case KIND_TIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readLocalTime",
            "(" + IN_DESC + ")Ljava/time/LocalTime;",
            false);
        break;
      case KIND_LOCAL_DATETIME:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readLocalDateTime",
            "(" + IN_DESC + ")Ljava/time/LocalDateTime;",
            false);
        break;
      case KIND_UUID:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readUuid",
            "(" + IN_DESC + ")Ljava/util/UUID;",
            false);
        break;
      case KIND_MICROS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readMicrosInstant",
            "(" + IN_DESC + ")Ljava/time/Instant;",
            false);
        break;
      case KIND_NANOS_INSTANT:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readNanosInstant",
            "(" + IN_DESC + ")Ljava/time/Instant;",
            false);
        break;
      case KIND_NANOS_DURATION:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readNanosDuration",
            "(" + IN_DESC + ")Ljava/time/Duration;",
            false);
        break;
      case KIND_STRING_LIST:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readStringList",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        break;
      case KIND_INT32_LIST:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readInt32List",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        break;
      case KIND_INT64_LIST:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readInt64List",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        break;
      case KIND_DOUBLE_LIST:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readDoubleList",
            "(" + IN_DESC + "[B)Ljava/util/List;",
            false);
        break;
      case KIND_ROW_LIST:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, elemCodersLocal);
        pushInt(mv, pos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readElementList",
            "(" + IN_DESC + CODER_DESC + ")Ljava/util/List;",
            false);
        break;
      case KIND_STRING_LONG_MAP:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readStringLongMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case KIND_STRING_STRING_MAP:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readStringStringMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case KIND_STRING_INT_MAP:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readStringIntMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      case KIND_STRING_DOUBLE_MAP:
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            ROW_WIRE_CODEC,
            "readStringDoubleMap",
            "(" + IN_DESC + "[B)Ljava/util/Map;",
            false);
        break;
      default:
        mv.visitVarInsn(Opcodes.ALOAD, codersLocal);
        pushInt(mv, pos);
        mv.visitInsn(Opcodes.AALOAD);
        mv.visitVarInsn(Opcodes.ALOAD, inLocal);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            CODER_TYPE,
            "decode",
            "(" + IN_DESC + ")Ljava/lang/Object;",
            false);
        break;
    }
  }

  private enum SourceKind {
    ARRAY,
    LIST,
    ROW
  }

  private static final class EncodeFieldsImpl implements Implementation {
    private final byte[] fieldKinds;
    private final int[] encodingPosToIndex;
    private final SourceKind sourceKind;
    private final boolean emitNullableHeader;

    EncodeFieldsImpl(
        byte[] fieldKinds,
        int[] encodingPosToIndex,
        SourceKind sourceKind,
        boolean emitNullableHeader) {
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.sourceKind = sourceKind;
      this.emitNullableHeader = emitNullableHeader;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    private void emitLoadField(MethodVisitor mv, int rowIndex) {
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      pushInt(mv, rowIndex);
      switch (sourceKind) {
        case ARRAY:
          mv.visitInsn(Opcodes.AALOAD);
          break;
        case LIST:
          mv.visitMethodInsn(
              Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
          break;
        case ROW:
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, ROW_TYPE, "getValue", "(I)Ljava/lang/Object;", false);
          break;
      }
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        int count = fieldKinds.length;
        // NoNulls:  0=this, 1=src, 2=out, 3=coders, 4=elemCoders, 5=staticEncoding, 6=fv, 7=pool,
        //           8=buf, 9=pos, 10=ex
        // Nullable: 0=this, 1=src, 2=out, 3=coders, 4=elemCoders, 5=fv, 6..7=nullMask, 8=pool,
        //           9=buf, 10=pos, 11=ex
        int fvLocal = emitNullableHeader ? 5 : 6;
        int poolLocal = emitNullableHeader ? 8 : 7;
        int bufLocal = poolLocal + 1;
        int posLocal = bufLocal + 1;
        int exLocal = posLocal + 1;

        Label tryStart = new Label();
        Label tryEnd = new Label();
        Label catchHandler = new Label();
        mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

        if (emitNullableHeader) {
          mv.visitInsn(Opcodes.LCONST_0);
          mv.visitVarInsn(Opcodes.LSTORE, 6);
          for (int pos = 0; pos < count; pos++) {
            Label notNull = new Label();
            emitLoadField(mv, encodingPosToIndex[pos]);
            mv.visitJumpInsn(Opcodes.IFNONNULL, notNull);
            mv.visitVarInsn(Opcodes.LLOAD, 6);
            mv.visitLdcInsn(1L << pos);
            mv.visitInsn(Opcodes.LOR);
            mv.visitVarInsn(Opcodes.LSTORE, 6);
            mv.visitLabel(notNull);
          }
        }

        // Check out a dedicated scratch buffer from the thread-local pool so re-entrant getters,
        // custom Row implementations, or nested coders can never alias this active buffer.
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
        mv.visitVarInsn(Opcodes.ASTORE, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
        mv.visitVarInsn(Opcodes.ASTORE, bufLocal);

        mv.visitLabel(tryStart);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, posLocal);

        if (emitNullableHeader) {
          pushInt(mv, count);
          mv.visitVarInsn(Opcodes.LLOAD, 6);
          emitPackCall(mv, "packHeader", "(IJ" + PACK_SUFFIX, bufLocal, posLocal, 2);
        } else {
          Label skipHeader = new Label();
          mv.visitVarInsn(Opcodes.ILOAD, 5);
          mv.visitJumpInsn(Opcodes.IFNE, skipHeader);
          pushInt(mv, count);
          mv.visitInsn(Opcodes.LCONST_0);
          emitPackCall(mv, "packHeader", "(IJ" + PACK_SUFFIX, bufLocal, posLocal, 2);
          mv.visitLabel(skipHeader);
        }

        for (int pos = 0; pos < count; pos++) {
          Label skip = new Label();
          emitLoadField(mv, encodingPosToIndex[pos]);
          mv.visitVarInsn(Opcodes.ASTORE, fvLocal);
          mv.visitVarInsn(Opcodes.ALOAD, fvLocal);
          mv.visitJumpInsn(Opcodes.IFNULL, skip);
          emitPackFieldValue(mv, fieldKinds[pos], pos, fvLocal, 2, 3, 4, bufLocal, posLocal);
          mv.visitLabel(skip);
        }
        emitFlush(mv, bufLocal, posLocal, 2);
        mv.visitLabel(tryEnd);

        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitInsn(Opcodes.RETURN);

        mv.visitLabel(catchHandler);
        mv.visitVarInsn(Opcodes.ASTORE, exLocal);
        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitVarInsn(Opcodes.ALOAD, exLocal);
        mv.visitInsn(Opcodes.ATHROW);
        return new ByteCodeAppender.Size(8, exLocal + 1);
      };
    }
  }

  private static boolean needsScratchForDecode(byte[] fieldKinds) {
    for (byte kind : fieldKinds) {
      switch (kind) {
        case KIND_INT16:
        case KIND_FLOAT:
        case KIND_DOUBLE:
        case KIND_DATETIME:
        case KIND_STRING:
        case KIND_DECIMAL:
        case KIND_STRING_LIST:
        case KIND_INT32_LIST:
        case KIND_INT64_LIST:
        case KIND_DOUBLE_LIST:
        case KIND_STRING_LONG_MAP:
        case KIND_STRING_STRING_MAP:
        case KIND_STRING_INT_MAP:
        case KIND_STRING_DOUBLE_MAP:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  private static final class DecodeImpl implements Implementation {
    private final byte[] fieldKinds;
    private final int[] encodingPosToIndex;
    private final boolean nullable;

    DecodeImpl(byte[] fieldKinds, int[] encodingPosToIndex, boolean nullable) {
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.nullable = nullable;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        int count = fieldKinds.length;
        // Nullable: 0=this, 1=in, 2=coders, 3=elemCoders, 4..5=nullMask, 6=buf (owned by caller),
        //           7=res
        // NoNulls:  0=this, 1=in, 2=coders, 3=elemCoders, 4=res, [5=pool, 6=buf, 7=ex]
        // Note: buf holds no live bytes across field boundaries, so nested sub-coder decodes cannot
        // observe or corrupt outer decode state (and ScratchPool gives nested decoders distinct
        // buffers anyway).
        if (nullable) {
          int bufLocal = 6;
          int resLocal = 7;
          pushInt(mv, count);
          mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
          mv.visitVarInsn(Opcodes.ASTORE, resLocal);
          for (int pos = 0; pos < count; pos++) {
            Label skip = new Label();
            mv.visitVarInsn(Opcodes.LLOAD, 4);
            mv.visitLdcInsn(1L << pos);
            mv.visitInsn(Opcodes.LAND);
            mv.visitInsn(Opcodes.LCONST_0);
            mv.visitInsn(Opcodes.LCMP);
            mv.visitJumpInsn(Opcodes.IFNE, skip);
            mv.visitVarInsn(Opcodes.ALOAD, resLocal);
            pushInt(mv, encodingPosToIndex[pos]);
            emitReadFieldValue(mv, fieldKinds[pos], pos, 1, 2, 3, bufLocal);
            mv.visitInsn(Opcodes.AASTORE);
            mv.visitLabel(skip);
          }
          mv.visitVarInsn(Opcodes.ALOAD, resLocal);
          mv.visitInsn(Opcodes.ARETURN);
          return new ByteCodeAppender.Size(6, resLocal + 1);
        }

        boolean needsScratch = needsScratchForDecode(fieldKinds);
        int resLocal = 4;
        if (!needsScratch) {
          pushInt(mv, count);
          mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
          mv.visitVarInsn(Opcodes.ASTORE, resLocal);
          for (int pos = 0; pos < count; pos++) {
            mv.visitVarInsn(Opcodes.ALOAD, resLocal);
            pushInt(mv, encodingPosToIndex[pos]);
            emitReadFieldValue(mv, fieldKinds[pos], pos, 1, 2, 3, -1);
            mv.visitInsn(Opcodes.AASTORE);
          }
          mv.visitVarInsn(Opcodes.ALOAD, resLocal);
          mv.visitInsn(Opcodes.ARETURN);
          return new ByteCodeAppender.Size(6, resLocal + 1);
        }

        int poolLocal = 5;
        int bufLocal = 6;
        int exLocal = 7;
        Label tryStart = new Label();
        Label tryEnd = new Label();
        Label catchHandler = new Label();
        mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, null);

        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "scratchPool", "()" + SCRATCH_POOL_DESC, false);
        mv.visitVarInsn(Opcodes.ASTORE, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "acquire", "()[B", false);
        mv.visitVarInsn(Opcodes.ASTORE, bufLocal);

        mv.visitLabel(tryStart);
        pushInt(mv, count);
        mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
        mv.visitVarInsn(Opcodes.ASTORE, resLocal);
        for (int pos = 0; pos < count; pos++) {
          mv.visitVarInsn(Opcodes.ALOAD, resLocal);
          pushInt(mv, encodingPosToIndex[pos]);
          emitReadFieldValue(mv, fieldKinds[pos], pos, 1, 2, 3, bufLocal);
          mv.visitInsn(Opcodes.AASTORE);
        }
        mv.visitLabel(tryEnd);

        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitVarInsn(Opcodes.ALOAD, resLocal);
        mv.visitInsn(Opcodes.ARETURN);

        mv.visitLabel(catchHandler);
        mv.visitVarInsn(Opcodes.ASTORE, exLocal);
        mv.visitVarInsn(Opcodes.ALOAD, poolLocal);
        mv.visitVarInsn(Opcodes.ALOAD, bufLocal);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, SCRATCH_POOL, "release", "([B)V", false);
        mv.visitVarInsn(Opcodes.ALOAD, exLocal);
        mv.visitInsn(Opcodes.ATHROW);
        return new ByteCodeAppender.Size(6, exLocal + 1);
      };
    }
  }

  private static final class EncodeChunkImpl implements Implementation {
    private final byte[] fieldKinds;
    private final int[] encodingPosToIndex;
    private final int start;
    private final int end;
    private final SourceKind sourceKind;

    EncodeChunkImpl(
        byte[] fieldKinds, int[] encodingPosToIndex, int start, int end, SourceKind sourceKind) {
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.start = start;
      this.end = end;
      this.sourceKind = sourceKind;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        // Static method slots: 0=src, 1=buf, 2=pos, 3=out, 4=coders, 5=elemCoders, 6=fv
        int fvLocal = 6;
        for (int pos = start; pos < end; pos++) {
          Label skip = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          pushInt(mv, encodingPosToIndex[pos]);
          switch (sourceKind) {
            case ARRAY:
              mv.visitInsn(Opcodes.AALOAD);
              break;
            case LIST:
              mv.visitMethodInsn(
                  Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
              break;
            case ROW:
              mv.visitMethodInsn(
                  Opcodes.INVOKEVIRTUAL, ROW_TYPE, "getValue", "(I)Ljava/lang/Object;", false);
              break;
          }
          mv.visitVarInsn(Opcodes.ASTORE, fvLocal);
          mv.visitVarInsn(Opcodes.ALOAD, fvLocal);
          mv.visitJumpInsn(Opcodes.IFNULL, skip);
          emitPackFieldValue(mv, fieldKinds[pos], pos, fvLocal, 3, 4, 5, 1, 2);
          mv.visitLabel(skip);
        }
        mv.visitVarInsn(Opcodes.ILOAD, 2);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(8, fvLocal + 1);
      };
    }
  }

  private static final class ChunkedEncodeDispatchImpl implements Implementation {
    private final int chunkCount;
    private final SourceKind sourceKind;

    ChunkedEncodeDispatchImpl(int chunkCount, SourceKind sourceKind) {
      this.chunkCount = chunkCount;
      this.sourceKind = sourceKind;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        String ownerInternal = target.getInstrumentedType().getInternalName();
        String prefix;
        String srcDesc;
        switch (sourceKind) {
          case ARRAY:
            prefix = "encArrC";
            srcDesc = "[Ljava/lang/Object;";
            break;
          case LIST:
            prefix = "encListC";
            srcDesc = "Ljava/util/List;";
            break;
          default:
            prefix = "encRowC";
            srcDesc = "L" + ROW_TYPE + ";";
            break;
        }
        String chunkDesc =
            "(" + srcDesc + "[BI" + OUT_DESC + "[" + CODER_DESC + "[" + CODER_DESC + ")I";
        // Instance slots: 0=this, 1=src, 2=out, 3=coders, 4=elemCoders, 5=buf, 6=pos
        for (int c = 0; c < chunkCount; c++) {
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ILOAD, 6);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, ownerInternal, prefix + c, chunkDesc, false);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
        }
        mv.visitVarInsn(Opcodes.ILOAD, 6);
        mv.visitInsn(Opcodes.IRETURN);
        return new ByteCodeAppender.Size(6, 7);
      };
    }
  }

  private static final class DecodeChunkImpl implements Implementation {
    private final byte[] fieldKinds;
    private final int[] encodingPosToIndex;
    private final int start;
    private final int end;
    private final boolean nullable;

    DecodeChunkImpl(
        byte[] fieldKinds, int[] encodingPosToIndex, int start, int end, boolean nullable) {
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.start = start;
      this.end = end;
      this.nullable = nullable;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        // Static slots: 0=res, 1=in, 2=coders, 3=elemCoders, 4=buf [, 5=chunkMask]
        if (nullable) {
          for (int pos = start; pos < end; pos++) {
            int bit = pos - start;
            Label skip = new Label();
            mv.visitVarInsn(Opcodes.ILOAD, 5);
            pushInt(mv, 1 << bit);
            mv.visitInsn(Opcodes.IAND);
            mv.visitJumpInsn(Opcodes.IFNE, skip);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            pushInt(mv, encodingPosToIndex[pos]);
            emitReadFieldValue(mv, fieldKinds[pos], pos, 1, 2, 3, 4);
            mv.visitInsn(Opcodes.AASTORE);
            mv.visitLabel(skip);
          }
          mv.visitInsn(Opcodes.RETURN);
          return new ByteCodeAppender.Size(6, 6);
        }

        for (int pos = start; pos < end; pos++) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          pushInt(mv, encodingPosToIndex[pos]);
          emitReadFieldValue(mv, fieldKinds[pos], pos, 1, 2, 3, 4);
          mv.visitInsn(Opcodes.AASTORE);
        }
        mv.visitInsn(Opcodes.RETURN);
        return new ByteCodeAppender.Size(6, 5);
      };
    }
  }

  private static final class ChunkedDecodeDispatchImpl implements Implementation {
    private final int fieldCount;
    private final int chunkCount;
    private final boolean nullable;

    ChunkedDecodeDispatchImpl(int fieldCount, int chunkCount, boolean nullable) {
      this.fieldCount = fieldCount;
      this.chunkCount = chunkCount;
      this.nullable = nullable;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target target) {
      return (mv, context, instrumentedMethod) -> {
        String ownerInternal = target.getInstrumentedType().getInternalName();
        String noNullDesc =
            "([Ljava/lang/Object;" + IN_DESC + "[" + CODER_DESC + "[" + CODER_DESC + "[B)V";
        if (!nullable) {
          // Instance slots: 0=this, 1=in, 2=coders, 3=elemCoders, 4=buf, 5=res
          pushInt(mv, fieldCount);
          mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
          mv.visitVarInsn(Opcodes.ASTORE, 5);
          for (int c = 0; c < chunkCount; c++) {
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitMethodInsn(
                Opcodes.INVOKESTATIC, ownerInternal, "decNoNullC" + c, noNullDesc, false);
          }
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitInsn(Opcodes.ARETURN);
          return new ByteCodeAppender.Size(6, 6);
        }

        // Instance slots: 0=this, 1=in, 2=coders, 3=elemCoders, 4=nullBytes, 5=nullByteLen,
        //                 6=buf, 7=res, 8=chunkMask
        String nullDesc =
            "([Ljava/lang/Object;" + IN_DESC + "[" + CODER_DESC + "[" + CODER_DESC + "[BI)V";
        pushInt(mv, fieldCount);
        mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
        mv.visitVarInsn(Opcodes.ASTORE, 7);
        for (int c = 0; c < chunkCount; c++) {
          Label chunkHasNulls = new Label();
          Label nextChunk = new Label();
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitVarInsn(Opcodes.ILOAD, 5);
          pushInt(mv, c);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, ROW_WIRE_CODEC, "extractChunkNullMask32", "([BII)I", false);
          mv.visitVarInsn(Opcodes.ISTORE, 8);
          mv.visitVarInsn(Opcodes.ILOAD, 8);
          mv.visitJumpInsn(Opcodes.IFNE, chunkHasNulls);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitMethodInsn(
              Opcodes.INVOKESTATIC, ownerInternal, "decNoNullC" + c, noNullDesc, false);
          mv.visitJumpInsn(Opcodes.GOTO, nextChunk);
          mv.visitLabel(chunkHasNulls);
          mv.visitVarInsn(Opcodes.ALOAD, 7);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitVarInsn(Opcodes.ALOAD, 3);
          mv.visitVarInsn(Opcodes.ALOAD, 6);
          mv.visitVarInsn(Opcodes.ILOAD, 8);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, ownerInternal, "decNullC" + c, nullDesc, false);
          mv.visitLabel(nextChunk);
        }
        mv.visitVarInsn(Opcodes.ALOAD, 7);
        mv.visitInsn(Opcodes.ARETURN);
        return new ByteCodeAppender.Size(6, 9);
      };
    }
  }

  /**
   * Concrete, monomorphic {@link Coder} for {@link Row} keeping 100% bit-for-bit compatibility with
   * {@code beam:coder:row:v1}.
   */
  private static final class GeneratedRowCoder extends CustomCoder<Row> {
    private final Schema schema;
    private final Coder[] coders;
    private final @Nullable Coder[] elemCoders;
    private final byte[] fieldKinds;
    private final int[] encodingPosToIndex;
    private final int fieldCount;
    private final boolean hasNullableFields;
    private final boolean identityPositions;
    private final boolean schemaStaticEncoding;
    // >= 0 iff !hasNullableFields and every field is a fixed-width primitive (payload size only,
    // excluding optional header bytes).
    private final long fixedPayloadSize;
    private final long defaultHeaderSize;
    // Transient: dynamically generated ByteBuddy subclass is not Serializable; readResolve
    // re-acquires the cached GeneratedRowCoder (including its fieldCodec) after deserialization.
    private transient @Nullable RowFieldCodec fieldCodec;
    private final transient boolean chunkedFieldCodec;
    private final boolean needsDecodeScratch;
    // Encoding positions that may fail isRegisterByteSizeObserverCheap (STRING, containers, ROW,
    // OTHER); null when every field is unconditionally O(1) cheap to size.
    private final @Nullable int[] cheapCheckPositions;

    GeneratedRowCoder(
        Schema schema,
        Coder[] coders,
        @Nullable Coder[] elemCoders,
        byte[] fieldKinds,
        int[] encodingPosToIndex,
        boolean hasNullableFields,
        boolean identityPositions,
        long fixedPayloadSize,
        @Nullable RowFieldCodec fieldCodec) {
      this.schema = schema;
      this.coders = coders;
      this.elemCoders = elemCoders;
      this.fieldKinds = fieldKinds;
      this.encodingPosToIndex = encodingPosToIndex;
      this.fieldCount = coders.length;
      this.hasNullableFields = hasNullableFields;
      this.identityPositions = identityPositions;
      this.schemaStaticEncoding = schema.isStaticEncoding();
      this.fixedPayloadSize = fixedPayloadSize;
      // Non-static non-null header: VarInt(fieldCount) + 1 byte (0x00 = empty null bitmap).
      this.defaultHeaderSize = VarInt.getLength(fieldCount) + 1L;
      this.fieldCodec = fieldCodec;
      this.chunkedFieldCodec = fieldCodec != null && this.fieldCount > MAX_UNROLLED_FIELDS;
      this.needsDecodeScratch = needsScratchForDecode(fieldKinds);
      this.cheapCheckPositions = computeCheapCheckPositions(fieldKinds);
    }

    private static @Nullable int[] computeCheapCheckPositions(byte[] fieldKinds) {
      int n = 0;
      for (byte kind : fieldKinds) {
        if (!isAlwaysCheapKind(kind)) {
          n++;
        }
      }
      if (n == 0) {
        return null;
      }
      int[] positions = new int[n];
      int out = 0;
      for (int pos = 0; pos < fieldKinds.length; pos++) {
        if (!isAlwaysCheapKind(fieldKinds[pos])) {
          positions[out++] = pos;
        }
      }
      return positions;
    }

    private static boolean isAlwaysCheapKind(byte kind) {
      switch (kind) {
        case KIND_BYTE:
        case KIND_INT16:
        case KIND_INT32:
        case KIND_INT64:
        case KIND_FLOAT:
        case KIND_DOUBLE:
        case KIND_BOOLEAN:
        case KIND_DATETIME:
        case KIND_BYTES:
        case KIND_DECIMAL:
        case KIND_DATE:
        case KIND_TIME:
        case KIND_LOCAL_DATETIME:
        case KIND_UUID:
        case KIND_MICROS_INSTANT:
        case KIND_NANOS_INSTANT:
        case KIND_NANOS_DURATION:
          return true;
        default:
          return false;
      }
    }

    private Object readResolve() {
      return schema.getUUID() != null ? RowCoderGenerator.generate(schema) : this;
    }

    @Override
    public void encode(Row value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null Row");
      }
      int count = fieldCount;
      checkState(value.getFieldCount() == value.getSchema().getFieldCount());
      checkState(count == value.getFieldCount());

      boolean staticEncoding = value.getSchema().isStaticEncoding();
      if (!hasNullableFields) {
        encodeNonNullFields(value, outStream, staticEncoding);
        return;
      }

      if (staticEncoding) {
        // Static encoding emits neither field count nor null bitmap, so encode in a single pass.
        encodeNonNullFields(value, outStream, true);
        return;
      }

      RowFieldCodec codec = fieldCodec;
      boolean singleMethod = codec != null && !chunkedFieldCodec;
      if (value instanceof RowWithStorage) {
        RowWithStorage storage = (RowWithStorage) value;
        Object[] arr = storage.getRawArray();
        if (arr != null) {
          if (singleMethod) {
            codec.encodeArrayNullable(arr, outStream, coders, elemCoders);
          } else {
            encodeNullableArray(arr, outStream);
          }
          return;
        }
        List<Object> list = storage.getValues();
        if (list instanceof RandomAccess) {
          if (singleMethod) {
            codec.encodeListNullable(list, outStream, coders, elemCoders);
          } else {
            encodeNullableList(list, outStream);
          }
        } else {
          Object[] extracted = list.toArray();
          if (singleMethod) {
            codec.encodeArrayNullable(extracted, outStream, coders, elemCoders);
          } else {
            encodeNullableArray(extracted, outStream);
          }
        }
        return;
      }

      Object[] extracted = new Object[count];
      for (int i = 0; i < count; i++) {
        extracted[i] = value.getValue(i);
      }
      if (singleMethod) {
        codec.encodeArrayNullable(extracted, outStream, coders, elemCoders);
      } else {
        encodeNullableArray(extracted, outStream);
      }
    }

    private void encodeNonNullFields(Row value, OutputStream out, boolean staticEncoding)
        throws IOException {
      int count = fieldCount;
      if (count == 0) {
        if (!staticEncoding) {
          RowWireCodec.writeHeader(0, 0L, out);
        }
        return;
      }
      RowFieldCodec codec = fieldCodec;
      boolean singleMethod = codec != null && !chunkedFieldCodec;
      Coder[] localCoders = coders;
      Coder[] localElemCoders = elemCoders;
      if (value instanceof RowWithStorage) {
        RowWithStorage storage = (RowWithStorage) value;
        Object[] arr = storage.getRawArray();
        if (arr != null) {
          if (singleMethod) {
            codec.encodeArrayNoNulls(arr, out, localCoders, localElemCoders, staticEncoding);
            return;
          }
          encodeNonNullArrayWide(arr, out, staticEncoding);
          return;
        }
        List<Object> list = storage.getValues();
        if (list instanceof RandomAccess) {
          if (singleMethod) {
            codec.encodeListNoNulls(list, out, localCoders, localElemCoders, staticEncoding);
            return;
          }
          encodeNonNullListWide(list, out, staticEncoding);
          return;
        }
        Object[] extracted = list.toArray();
        if (singleMethod) {
          codec.encodeArrayNoNulls(extracted, out, localCoders, localElemCoders, staticEncoding);
          return;
        }
        encodeNonNullArrayWide(extracted, out, staticEncoding);
        return;
      }

      if (singleMethod) {
        codec.encodeRowNoNulls(value, out, localCoders, localElemCoders, staticEncoding);
        return;
      }
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        int bufPos = staticEncoding ? 0 : RowWireCodec.packHeader(count, 0L, buf, 0, out);
        if (codec != null && codec.supportsRowBody()) {
          bufPos = codec.encodeRowBody(value, out, localCoders, localElemCoders, buf, bufPos);
        } else {
          byte[] kinds = fieldKinds;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < count; pos++) {
            Object fv = value.getValue(identityPositions ? pos : map[pos]);
            if (fv != null) {
              bufPos =
                  packField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      fv,
                      buf,
                      bufPos,
                      out);
            }
          }
        }
        RowWireCodec.flush(buf, bufPos, out);
      } finally {
        pool.release(buf);
      }
    }

    private void encodeNonNullArrayWide(Object[] arr, OutputStream out, boolean staticEncoding)
        throws IOException {
      int count = fieldCount;
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        int bufPos = staticEncoding ? 0 : RowWireCodec.packHeader(count, 0L, buf, 0, out);
        RowFieldCodec codec = fieldCodec;
        Coder[] localCoders = coders;
        Coder[] localElemCoders = elemCoders;
        if (codec != null && chunkedFieldCodec) {
          bufPos = codec.encodeArrayBody(arr, out, localCoders, localElemCoders, buf, bufPos);
        } else {
          byte[] kinds = fieldKinds;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < count; pos++) {
            Object fv = arr[identityPositions ? pos : map[pos]];
            if (fv != null) {
              bufPos =
                  packField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      fv,
                      buf,
                      bufPos,
                      out);
            }
          }
        }
        RowWireCodec.flush(buf, bufPos, out);
      } finally {
        pool.release(buf);
      }
    }

    private void encodeNonNullListWide(List<Object> list, OutputStream out, boolean staticEncoding)
        throws IOException {
      int count = fieldCount;
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        int bufPos = staticEncoding ? 0 : RowWireCodec.packHeader(count, 0L, buf, 0, out);
        RowFieldCodec codec = fieldCodec;
        Coder[] localCoders = coders;
        Coder[] localElemCoders = elemCoders;
        if (codec != null && chunkedFieldCodec) {
          bufPos = codec.encodeListBody(list, out, localCoders, localElemCoders, buf, bufPos);
        } else {
          byte[] kinds = fieldKinds;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < count; pos++) {
            Object fv = list.get(identityPositions ? pos : map[pos]);
            if (fv != null) {
              bufPos =
                  packField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      fv,
                      buf,
                      bufPos,
                      out);
            }
          }
        }
        RowWireCodec.flush(buf, bufPos, out);
      } finally {
        pool.release(buf);
      }
    }

    private void encodeNullableArray(Object[] arr, OutputStream out) throws IOException {
      int count = fieldCount;
      Coder[] localCoders = coders;
      Coder[] localElemCoders = elemCoders;
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        int bufPos = encodeNullableHeader(arr, null, count, buf, out);
        RowFieldCodec codec = fieldCodec;
        if (codec != null && chunkedFieldCodec) {
          bufPos = codec.encodeArrayBody(arr, out, localCoders, localElemCoders, buf, bufPos);
        } else {
          byte[] kinds = fieldKinds;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < count; pos++) {
            Object v = arr[identityPositions ? pos : map[pos]];
            if (v != null) {
              bufPos =
                  packField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      v,
                      buf,
                      bufPos,
                      out);
            }
          }
        }
        RowWireCodec.flush(buf, bufPos, out);
      } finally {
        pool.release(buf);
      }
    }

    private void encodeNullableList(List<Object> list, OutputStream out) throws IOException {
      int count = fieldCount;
      Coder[] localCoders = coders;
      Coder[] localElemCoders = elemCoders;
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        int bufPos = encodeNullableHeader(null, list, count, buf, out);
        RowFieldCodec codec = fieldCodec;
        if (codec != null && chunkedFieldCodec) {
          bufPos = codec.encodeListBody(list, out, localCoders, localElemCoders, buf, bufPos);
        } else {
          byte[] kinds = fieldKinds;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < count; pos++) {
            Object v = list.get(identityPositions ? pos : map[pos]);
            if (v != null) {
              bufPos =
                  packField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      v,
                      buf,
                      bufPos,
                      out);
            }
          }
        }
        RowWireCodec.flush(buf, bufPos, out);
      } finally {
        pool.release(buf);
      }
    }

    private int encodeNullableHeader(
        @Nullable Object[] arr,
        @Nullable List<Object> list,
        int count,
        byte[] buf,
        OutputStream out)
        throws IOException {
      int[] map = encodingPosToIndex;
      if (count <= 64) {
        long nullMask = 0L;
        for (int pos = 0; pos < count; pos++) {
          int idx = identityPositions ? pos : map[pos];
          Object v = arr != null ? arr[idx] : list.get(idx);
          if (v == null) {
            nullMask |= (1L << pos);
          }
        }
        return RowWireCodec.packHeader(count, nullMask, buf, 0, out);
      }
      if (arr != null) {
        return RowWireCodec.packWideNullHeaderFromArray(
            arr, map, identityPositions, count, buf, out);
      }
      return RowWireCodec.packWideNullHeaderFromList(list, map, identityPositions, count, buf, out);
    }

    @Override
    public Row decode(InputStream inStream) throws IOException {
      if (schemaStaticEncoding) {
        int localFieldCount = fieldCount;
        if (localFieldCount == 0) {
          return Row.attachValues(schema, EMPTY_VALUES);
        }
        RowFieldCodec codec = fieldCodec;
        if (codec != null) {
          if (!chunkedFieldCodec) {
            return Row.attachValues(schema, codec.decodeNoNulls(inStream, coders, elemCoders));
          }
          if (!needsDecodeScratch) {
            return Row.attachValues(
                schema, codec.decodeNoNullsWide(inStream, coders, elemCoders, null));
          }
          RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
          byte[] buf = pool.acquire();
          try {
            return Row.attachValues(
                schema, codec.decodeNoNullsWide(inStream, coders, elemCoders, buf));
          } finally {
            pool.release(buf);
          }
        }
        Object[] fieldValues = new Object[localFieldCount];
        decodeNoNullLoop(fieldValues, localFieldCount, inStream);
        return Row.attachValues(schema, fieldValues);
      }

      int streamFieldCount = RowWireCodec.readVarInt(inStream);
      int nullByteLen = RowWireCodec.readVarInt(inStream);
      return decodeAfterNullByteLen(streamFieldCount, nullByteLen, inStream);
    }

    Row decodeAfterNullByteLen(int streamFieldCount, int nullByteLen, InputStream inStream)
        throws IOException {
      if (nullByteLen < 0) {
        throw new CoderException("Invalid null bitset length: " + nullByteLen);
      }
      int localFieldCount = fieldCount;
      if (localFieldCount == 0) {
        if (nullByteLen > 0) {
          RowWireCodec.skipExact(inStream, nullByteLen);
        }
        return Row.attachValues(schema, EMPTY_VALUES);
      }

      RowFieldCodec codec = fieldCodec;
      boolean hasChunked = codec != null && chunkedFieldCodec;
      if (nullByteLen == 0) {
        if (codec != null && streamFieldCount >= localFieldCount) {
          if (!hasChunked) {
            return Row.attachValues(schema, codec.decodeNoNulls(inStream, coders, elemCoders));
          }
          if (!needsDecodeScratch) {
            return Row.attachValues(
                schema, codec.decodeNoNullsWide(inStream, coders, elemCoders, null));
          }
          RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
          byte[] buf = pool.acquire();
          try {
            return Row.attachValues(
                schema, codec.decodeNoNullsWide(inStream, coders, elemCoders, buf));
          } finally {
            pool.release(buf);
          }
        }
        Object[] fieldValues = new Object[localFieldCount];
        int decodeCount = Math.min(streamFieldCount, localFieldCount);
        decodeNoNullLoop(fieldValues, decodeCount, inStream);
        return Row.attachValues(schema, fieldValues);
      }

      if (nullByteLen <= 8 && !hasChunked) {
        RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
        byte[] buf = pool.acquire();
        try {
          long nullMask = RowWireCodec.readNullMask(inStream, nullByteLen, buf);
          if (codec != null && streamFieldCount >= localFieldCount) {
            return Row.attachValues(
                schema, codec.decodeNullable(inStream, coders, elemCoders, nullMask, buf));
          }
          Object[] fieldValues = new Object[localFieldCount];
          int decodeCount = Math.min(streamFieldCount, localFieldCount);
          byte[] kinds = fieldKinds;
          Coder[] localCoders = coders;
          Coder[] localElemCoders = elemCoders;
          int[] map = encodingPosToIndex;
          for (int pos = 0; pos < decodeCount; pos++) {
            if (pos >= 64 || (nullMask & (1L << pos)) == 0L) {
              fieldValues[identityPositions ? pos : map[pos]] =
                  decodeField(
                      kinds[pos],
                      localCoders[pos],
                      localElemCoders != null ? localElemCoders[pos] : null,
                      inStream,
                      buf);
            }
          }
          return Row.attachValues(schema, fieldValues);
        } finally {
          pool.release(buf);
        }
      }

      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] nullScratch = pool.acquire();
      @Nullable byte[] buf = null;
      try {
        if (needsDecodeScratch) {
          buf = pool.acquire();
        }
        byte[] nullBytes = RowWireCodec.readNullBytes(inStream, nullByteLen, nullScratch);
        if (hasChunked
            && codec.supportsNullableWideDecode()
            && streamFieldCount >= localFieldCount) {
          return Row.attachValues(
              schema,
              codec.decodeNullableWide(inStream, coders, elemCoders, nullBytes, nullByteLen, buf));
        }
        Object[] fieldValues = new Object[localFieldCount];
        int decodeCount = Math.min(streamFieldCount, localFieldCount);
        byte[] kinds = fieldKinds;
        Coder[] localCoders = coders;
        Coder[] localElemCoders = elemCoders;
        int[] map = encodingPosToIndex;
        for (int pos = 0; pos < decodeCount; pos++) {
          if (!RowWireCodec.isNullBitSet(nullBytes, nullByteLen, pos)) {
            fieldValues[identityPositions ? pos : map[pos]] =
                decodeField(
                    kinds[pos],
                    localCoders[pos],
                    localElemCoders != null ? localElemCoders[pos] : null,
                    inStream,
                    buf);
          }
        }
        return Row.attachValues(schema, fieldValues);
      } finally {
        if (buf != null) {
          pool.release(buf);
        }
        pool.release(nullScratch);
      }
    }

    private void decodeNoNullLoop(Object[] fieldValues, int decodeCount, InputStream inStream)
        throws IOException {
      byte[] kinds = fieldKinds;
      Coder[] localCoders = coders;
      Coder[] localElemCoders = elemCoders;
      int[] map = encodingPosToIndex;
      if (!needsDecodeScratch) {
        for (int pos = 0; pos < decodeCount; pos++) {
          fieldValues[identityPositions ? pos : map[pos]] =
              decodeField(
                  kinds[pos],
                  localCoders[pos],
                  localElemCoders != null ? localElemCoders[pos] : null,
                  inStream,
                  null);
        }
        return;
      }
      RowWireCodec.ScratchPool pool = RowWireCodec.scratchPool();
      byte[] buf = pool.acquire();
      try {
        for (int pos = 0; pos < decodeCount; pos++) {
          fieldValues[identityPositions ? pos : map[pos]] =
              decodeField(
                  kinds[pos],
                  localCoders[pos],
                  localElemCoders != null ? localElemCoders[pos] : null,
                  inStream,
                  buf);
        }
      } finally {
        pool.release(buf);
      }
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Row value) {
      if (value == null) {
        return false;
      }
      @Nullable int[] checkPositions = cheapCheckPositions;
      if (checkPositions == null) {
        return true;
      }
      byte[] kinds = fieldKinds;
      Coder[] localCoders = coders;
      int[] map = encodingPosToIndex;
      for (int pos : checkPositions) {
        Object fv = value.getValue(identityPositions ? pos : map[pos]);
        if (fv == null) {
          continue;
        }
        if (kinds[pos] == KIND_STRING) {
          if (((String) fv).length() > 1024) {
            return false;
          }
        } else if (!localCoders[pos].isRegisterByteSizeObserverCheap(fv)) {
          return false;
        }
      }
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerByteSizeObserver(Row value, ElementByteSizeObserver observer)
        throws Exception {
      if (value == null) {
        throw new CoderException("cannot observe a null Row");
      }
      boolean staticEncoding = value.getSchema().isStaticEncoding();
      if (fixedPayloadSize >= 0L) {
        observer.update(staticEncoding ? fixedPayloadSize : (defaultHeaderSize + fixedPayloadSize));
        return;
      }

      int count = fieldCount;
      checkState(value.getFieldCount() == value.getSchema().getFieldCount());
      checkState(count == value.getFieldCount());

      long directBytes = 0L;
      int highestNullPos = -1;
      byte[] kinds = fieldKinds;
      Coder[] localCoders = coders;
      int[] map = encodingPosToIndex;

      if (!staticEncoding) {
        directBytes += VarInt.getLength(count);
      }

      Object[] rawArr = null;
      List<?> rawList = null;
      if (value instanceof RowWithStorage) {
        RowWithStorage storage = (RowWithStorage) value;
        rawArr = storage.getRawArray();
        if (rawArr == null) {
          List<Object> vals = storage.getValues();
          if (vals instanceof RandomAccess) {
            rawList = vals;
          }
        }
      }
      for (int pos = 0; pos < count; pos++) {
        int idx = identityPositions ? pos : map[pos];
        Object fv =
            rawArr != null
                ? rawArr[idx]
                : (rawList != null ? rawList.get(idx) : value.getValue(idx));
        if (fv == null) {
          if (hasNullableFields) {
            highestNullPos = pos;
          }
          continue;
        }
        switch (kinds[pos]) {
          case KIND_BYTE:
          case KIND_BOOLEAN:
            directBytes += 1L;
            break;
          case KIND_INT16:
            directBytes += 2L;
            break;
          case KIND_FLOAT:
            directBytes += 4L;
            break;
          case KIND_DOUBLE:
          case KIND_DATETIME:
            directBytes += 8L;
            break;
          case KIND_INT32:
            directBytes += VarInt.getLength(((Integer) fv).intValue());
            break;
          case KIND_INT64:
            directBytes += VarInt.getLength(((Long) fv).longValue());
            break;
          case KIND_BYTES:
            directBytes += RowWireCodec.sizeOfBytes((byte[]) fv);
            break;
          case KIND_STRING:
            directBytes += RowWireCodec.sizeOfString((String) fv);
            break;
          case KIND_DECIMAL:
            directBytes += RowWireCodec.sizeOfDecimal((BigDecimal) fv);
            break;
          case KIND_DATE:
            directBytes += RowWireCodec.sizeOfLocalDate((LocalDate) fv);
            break;
          case KIND_TIME:
            directBytes += RowWireCodec.sizeOfLocalTime((LocalTime) fv);
            break;
          case KIND_LOCAL_DATETIME:
            directBytes += RowWireCodec.sizeOfLocalDateTime((LocalDateTime) fv);
            break;
          case KIND_UUID:
            directBytes += RowWireCodec.sizeOfUuid((UUID) fv);
            break;
          case KIND_MICROS_INSTANT:
            directBytes += RowWireCodec.sizeOfMicrosInstant((java.time.Instant) fv);
            break;
          case KIND_NANOS_INSTANT:
            directBytes += RowWireCodec.sizeOfNanosInstant((java.time.Instant) fv);
            break;
          case KIND_NANOS_DURATION:
            directBytes += RowWireCodec.sizeOfNanosDuration((Duration) fv);
            break;
          case KIND_STRING_LIST:
            if (fv instanceof Collection && !(fv instanceof ElementByteSizeObservableIterable)) {
              directBytes += RowWireCodec.sizeOfStringCollection((Collection<String>) fv);
            } else {
              localCoders[pos].registerByteSizeObserver(fv, observer);
            }
            break;
          case KIND_INT32_LIST:
            if (fv instanceof Collection && !(fv instanceof ElementByteSizeObservableIterable)) {
              directBytes += RowWireCodec.sizeOfInt32Collection((Collection<Integer>) fv);
            } else {
              localCoders[pos].registerByteSizeObserver(fv, observer);
            }
            break;
          case KIND_INT64_LIST:
            if (fv instanceof Collection && !(fv instanceof ElementByteSizeObservableIterable)) {
              directBytes += RowWireCodec.sizeOfInt64Collection((Collection<Long>) fv);
            } else {
              localCoders[pos].registerByteSizeObserver(fv, observer);
            }
            break;
          case KIND_DOUBLE_LIST:
            if (fv instanceof Collection && !(fv instanceof ElementByteSizeObservableIterable)) {
              directBytes += RowWireCodec.sizeOfDoubleCollection((Collection<Double>) fv);
            } else {
              localCoders[pos].registerByteSizeObserver(fv, observer);
            }
            break;
          case KIND_STRING_LONG_MAP:
            directBytes += RowWireCodec.sizeOfStringLongMap((Map<String, Long>) fv);
            break;
          case KIND_STRING_STRING_MAP:
            directBytes += RowWireCodec.sizeOfStringStringMap((Map<String, String>) fv);
            break;
          case KIND_STRING_INT_MAP:
            directBytes += RowWireCodec.sizeOfStringIntMap((Map<String, Integer>) fv);
            break;
          case KIND_STRING_DOUBLE_MAP:
            directBytes += RowWireCodec.sizeOfStringDoubleMap((Map<String, Double>) fv);
            break;
          default:
            localCoders[pos].registerByteSizeObserver(fv, observer);
            break;
        }
      }

      if (!staticEncoding) {
        directBytes += RowWireCodec.sizeOfNullMask(highestNullPos);
      }
      if (directBytes > 0L || count == 0) {
        observer.update(directBytes);
      }
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      Coder.verifyDeterministic(
          this, "All fields must have deterministic encoding", Arrays.asList(coders));
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @SuppressWarnings("unchecked")
    private static int packField(
        byte kind,
        Coder coder,
        @Nullable Coder elemCoder,
        Object fieldValue,
        byte[] buf,
        int pos,
        OutputStream out)
        throws IOException {
      switch (kind) {
        case KIND_BYTE:
          return RowWireCodec.packByte(((Byte) fieldValue).byteValue(), buf, pos, out);
        case KIND_INT16:
          return RowWireCodec.packInt16(((Short) fieldValue).shortValue(), buf, pos, out);
        case KIND_INT32:
          return RowWireCodec.packVarInt(((Integer) fieldValue).intValue(), buf, pos, out);
        case KIND_INT64:
          return RowWireCodec.packVarLong(((Long) fieldValue).longValue(), buf, pos, out);
        case KIND_FLOAT:
          return RowWireCodec.packFloat(((Float) fieldValue).floatValue(), buf, pos, out);
        case KIND_DOUBLE:
          return RowWireCodec.packDouble(((Double) fieldValue).doubleValue(), buf, pos, out);
        case KIND_BOOLEAN:
          return RowWireCodec.packBoolean(((Boolean) fieldValue).booleanValue(), buf, pos, out);
        case KIND_DATETIME:
          return RowWireCodec.packDateTime((ReadableInstant) fieldValue, buf, pos, out);
        case KIND_STRING:
          return RowWireCodec.packString((String) fieldValue, buf, pos, out);
        case KIND_BYTES:
          return RowWireCodec.packBytes((byte[]) fieldValue, buf, pos, out);
        case KIND_DECIMAL:
          return RowWireCodec.packDecimal((BigDecimal) fieldValue, buf, pos, out);
        case KIND_DATE:
          return RowWireCodec.packLocalDate((LocalDate) fieldValue, buf, pos, out);
        case KIND_TIME:
          return RowWireCodec.packLocalTime((LocalTime) fieldValue, buf, pos, out);
        case KIND_LOCAL_DATETIME:
          return RowWireCodec.packLocalDateTime((LocalDateTime) fieldValue, buf, pos, out);
        case KIND_UUID:
          return RowWireCodec.packUuid((UUID) fieldValue, buf, pos, out);
        case KIND_MICROS_INSTANT:
          return RowWireCodec.packMicrosInstant((java.time.Instant) fieldValue, buf, pos, out);
        case KIND_NANOS_INSTANT:
          return RowWireCodec.packNanosInstant((java.time.Instant) fieldValue, buf, pos, out);
        case KIND_NANOS_DURATION:
          return RowWireCodec.packNanosDuration((Duration) fieldValue, buf, pos, out);
        case KIND_STRING_LIST:
          return RowWireCodec.packStringIterable(
              (Iterable<String>) fieldValue, buf, pos, out, coder);
        case KIND_INT32_LIST:
          return RowWireCodec.packInt32Iterable(
              (Iterable<Integer>) fieldValue, buf, pos, out, coder);
        case KIND_INT64_LIST:
          return RowWireCodec.packInt64Iterable((Iterable<Long>) fieldValue, buf, pos, out, coder);
        case KIND_DOUBLE_LIST:
          return RowWireCodec.packDoubleIterable(
              (Iterable<Double>) fieldValue, buf, pos, out, coder);
        case KIND_ROW_LIST:
          RowWireCodec.flush(buf, pos, out);
          RowWireCodec.writeElementIterable((Iterable) fieldValue, elemCoder, out, coder);
          return 0;
        case KIND_STRING_LONG_MAP:
          return RowWireCodec.packStringLongMap((Map<String, Long>) fieldValue, buf, pos, out);
        case KIND_STRING_STRING_MAP:
          return RowWireCodec.packStringStringMap((Map<String, String>) fieldValue, buf, pos, out);
        case KIND_STRING_INT_MAP:
          return RowWireCodec.packStringIntMap((Map<String, Integer>) fieldValue, buf, pos, out);
        case KIND_STRING_DOUBLE_MAP:
          return RowWireCodec.packStringDoubleMap((Map<String, Double>) fieldValue, buf, pos, out);
        case KIND_ROW:
        default:
          RowWireCodec.flush(buf, pos, out);
          coder.encode(fieldValue, out);
          return 0;
      }
    }

    private static Object decodeField(
        byte kind, Coder coder, @Nullable Coder elemCoder, InputStream in, byte[] buf)
        throws IOException {
      switch (kind) {
        case KIND_BYTE:
          return RowWireCodec.readByte(in);
        case KIND_INT16:
          return RowWireCodec.readInt16(in, buf);
        case KIND_INT32:
          return RowWireCodec.readVarInt(in);
        case KIND_INT64:
          return RowWireCodec.readVarLong(in);
        case KIND_FLOAT:
          return RowWireCodec.readFloat(in, buf);
        case KIND_DOUBLE:
          return RowWireCodec.readDouble(in, buf);
        case KIND_BOOLEAN:
          return RowWireCodec.readBoolean(in) ? Boolean.TRUE : Boolean.FALSE;
        case KIND_DATETIME:
          return RowWireCodec.readDateTime(in, buf);
        case KIND_STRING:
          return RowWireCodec.readString(in, buf);
        case KIND_BYTES:
          return RowWireCodec.readBytes(in);
        case KIND_DECIMAL:
          return RowWireCodec.readDecimal(in, buf);
        case KIND_DATE:
          return RowWireCodec.readLocalDate(in);
        case KIND_TIME:
          return RowWireCodec.readLocalTime(in);
        case KIND_LOCAL_DATETIME:
          return RowWireCodec.readLocalDateTime(in);
        case KIND_UUID:
          return RowWireCodec.readUuid(in);
        case KIND_MICROS_INSTANT:
          return RowWireCodec.readMicrosInstant(in);
        case KIND_NANOS_INSTANT:
          return RowWireCodec.readNanosInstant(in);
        case KIND_NANOS_DURATION:
          return RowWireCodec.readNanosDuration(in);
        case KIND_STRING_LIST:
          return RowWireCodec.readStringList(in, buf);
        case KIND_INT32_LIST:
          return RowWireCodec.readInt32List(in, buf);
        case KIND_INT64_LIST:
          return RowWireCodec.readInt64List(in, buf);
        case KIND_DOUBLE_LIST:
          return RowWireCodec.readDoubleList(in, buf);
        case KIND_ROW_LIST:
          return RowWireCodec.readElementList(in, elemCoder);
        case KIND_STRING_LONG_MAP:
          return RowWireCodec.readStringLongMap(in, buf);
        case KIND_STRING_STRING_MAP:
          return RowWireCodec.readStringStringMap(in, buf);
        case KIND_STRING_INT_MAP:
          return RowWireCodec.readStringIntMap(in, buf);
        case KIND_STRING_DOUBLE_MAP:
          return RowWireCodec.readStringDoubleMap(in, buf);
        case KIND_ROW:
        default:
          return coder.decode(in);
      }
    }
  }
}
