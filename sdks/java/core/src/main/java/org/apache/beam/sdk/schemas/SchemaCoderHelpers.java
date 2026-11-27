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
package org.apache.beam.sdk.schemas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.coders.RowWireCodec;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableInstant;

@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
class SchemaCoderHelpers {
  // This contains a map of primitive types to their coders.
  private static final Map<TypeName, Coder> CODER_MAP =
      ImmutableMap.<TypeName, Coder>builder()
          .put(TypeName.BYTE, ByteCoder.of())
          .put(TypeName.BYTES, ByteArrayCoder.of())
          .put(TypeName.INT16, BigEndianShortCoder.of())
          .put(TypeName.INT32, VarIntCoder.of())
          .put(TypeName.INT64, VarLongCoder.of())
          .put(TypeName.DECIMAL, BigDecimalCoder.of())
          .put(TypeName.FLOAT, FloatCoder.of())
          .put(TypeName.DOUBLE, DoubleCoder.of())
          .put(TypeName.STRING, StringUtf8Coder.of())
          .put(TypeName.DATETIME, InstantCoder.of())
          .put(TypeName.BOOLEAN, BooleanCoder.of())
          .build();

  private static boolean canUseDirectRowEncoding(Schema rowSchema) {
    return !rowSchema.isEncodingPositionsOverridden()
        && !rowSchema.isStaticEncoding()
        && !RowCoderGenerator.hasEncodingPositionOverride(rowSchema.getUUID());
  }

  private static boolean isBaseTypeAlwaysCheap(FieldType baseType) {
    switch (baseType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case DATETIME:
      case BYTES:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  private static boolean isIdentityPassThrough(LogicalType<?, ?> logicalType) {
    if (!(logicalType instanceof PassThroughLogicalType)) {
      return false;
    }
    // Preserve ReadableInstant -> Instant coercion handled inside LogicalTypeCoder.
    if (logicalType.getBaseType().getTypeName() == TypeName.DATETIME) {
      return false;
    }
    Class<?> clazz = logicalType.getClass();
    try {
      return clazz.getMethod("toBaseType", Object.class).getDeclaringClass()
              == PassThroughLogicalType.class
          && clazz.getMethod("toInputType", Object.class).getDeclaringClass()
              == PassThroughLogicalType.class;
    } catch (NoSuchMethodException | SecurityException e) {
      return false;
    }
  }

  /** Shared base for zero-Row-allocation specialized logical-type coders. */
  private abstract static class DirectLogicalTypeCoder<T> extends CustomCoder<T> {
    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      observer.update(getEncodedElementByteSize(value));
    }

    @Override
    @SuppressWarnings("EqualsGetClass")
    public boolean equals(@Nullable Object o) {
      return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  /** Direct zero-Row-alloc coder for {@link Date} matching {@code VarLongCoder} bytes. */
  private static final class DateLogicalTypeCoder extends DirectLogicalTypeCoder<LocalDate> {
    static final DateLogicalTypeCoder INSTANCE = new DateLogicalTypeCoder();

    @Override
    public void encode(LocalDate value, OutputStream outStream) throws IOException {
      RowWireCodec.writeLocalDate(value, outStream);
    }

    @Override
    public LocalDate decode(InputStream inStream) throws IOException {
      return RowWireCodec.readLocalDate(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(LocalDate value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null LocalDate");
      }
      return RowWireCodec.sizeOfLocalDate(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link Time} matching {@code VarLongCoder} bytes. */
  private static final class TimeLogicalTypeCoder extends DirectLogicalTypeCoder<LocalTime> {
    static final TimeLogicalTypeCoder INSTANCE = new TimeLogicalTypeCoder();

    @Override
    public void encode(LocalTime value, OutputStream outStream) throws IOException {
      RowWireCodec.writeLocalTime(value, outStream);
    }

    @Override
    public LocalTime decode(InputStream inStream) throws IOException {
      return RowWireCodec.readLocalTime(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(LocalTime value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null LocalTime");
      }
      return RowWireCodec.sizeOfLocalTime(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link DateTime} matching {@code beam:coder:row:v1}. */
  private static final class DateTimeLogicalTypeCoder
      extends DirectLogicalTypeCoder<LocalDateTime> {
    static final DateTimeLogicalTypeCoder INSTANCE = new DateTimeLogicalTypeCoder();

    @Override
    public void encode(LocalDateTime value, OutputStream outStream) throws IOException {
      RowWireCodec.writeLocalDateTime(value, outStream);
    }

    @Override
    public LocalDateTime decode(InputStream inStream) throws IOException {
      return RowWireCodec.readLocalDateTime(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(LocalDateTime value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null LocalDateTime");
      }
      return RowWireCodec.sizeOfLocalDateTime(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link UuidLogicalType} matching {@code beam:coder:row:v1}. */
  private static final class UuidLogicalTypeCoder extends DirectLogicalTypeCoder<UUID> {
    static final UuidLogicalTypeCoder INSTANCE = new UuidLogicalTypeCoder();

    @Override
    public void encode(UUID value, OutputStream outStream) throws IOException {
      RowWireCodec.writeUuid(value, outStream);
    }

    @Override
    public UUID decode(InputStream inStream) throws IOException {
      return RowWireCodec.readUuid(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(UUID value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null UUID");
      }
      return RowWireCodec.sizeOfUuid(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link Timestamp} matching {@code beam:coder:row:v1}. */
  private static final class TimestampLogicalTypeCoder extends DirectLogicalTypeCoder<Instant> {
    private final int precision;

    TimestampLogicalTypeCoder(int precision) {
      this.precision = precision;
    }

    @Override
    public void encode(Instant value, OutputStream outStream) throws IOException {
      RowWireCodec.writeTimestamp(value, precision, outStream);
    }

    @Override
    public Instant decode(InputStream inStream) throws IOException {
      return RowWireCodec.readTimestamp(precision, inStream);
    }

    @Override
    protected long getEncodedElementByteSize(Instant value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null Instant");
      }
      return RowWireCodec.sizeOfTimestamp(value, precision);
    }

    @Override
    @SuppressWarnings("EqualsGetClass")
    public boolean equals(@Nullable Object o) {
      return o instanceof TimestampLogicalTypeCoder
          && precision == ((TimestampLogicalTypeCoder) o).precision;
    }

    @Override
    public int hashCode() {
      return 31 * TimestampLogicalTypeCoder.class.hashCode() + precision;
    }
  }

  /** Direct zero-Row-alloc coder for {@link MicrosInstant} matching {@code beam:coder:row:v1}. */
  private static final class MicrosInstantLogicalTypeCoder extends DirectLogicalTypeCoder<Instant> {
    static final MicrosInstantLogicalTypeCoder INSTANCE = new MicrosInstantLogicalTypeCoder();

    @Override
    public void encode(Instant value, OutputStream outStream) throws IOException {
      RowWireCodec.writeMicrosInstant(value, outStream);
    }

    @Override
    public Instant decode(InputStream inStream) throws IOException {
      return RowWireCodec.readMicrosInstant(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(Instant value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null Instant");
      }
      return RowWireCodec.sizeOfMicrosInstant(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link NanosInstant} matching {@code beam:coder:row:v1}. */
  private static final class NanosInstantLogicalTypeCoder extends DirectLogicalTypeCoder<Instant> {
    static final NanosInstantLogicalTypeCoder INSTANCE = new NanosInstantLogicalTypeCoder();

    @Override
    public void encode(Instant value, OutputStream outStream) throws IOException {
      RowWireCodec.writeNanosInstant(value, outStream);
    }

    @Override
    public Instant decode(InputStream inStream) throws IOException {
      return RowWireCodec.readNanosInstant(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(Instant value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null Instant");
      }
      return RowWireCodec.sizeOfNanosInstant(value);
    }
  }

  /** Direct zero-Row-alloc coder for {@link NanosDuration} matching {@code beam:coder:row:v1}. */
  private static final class NanosDurationLogicalTypeCoder
      extends DirectLogicalTypeCoder<Duration> {
    static final NanosDurationLogicalTypeCoder INSTANCE = new NanosDurationLogicalTypeCoder();

    @Override
    public void encode(Duration value, OutputStream outStream) throws IOException {
      RowWireCodec.writeNanosDuration(value, outStream);
    }

    @Override
    public Duration decode(InputStream inStream) throws IOException {
      return RowWireCodec.readNanosDuration(inStream);
    }

    @Override
    protected long getEncodedElementByteSize(Duration value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null Duration");
      }
      return RowWireCodec.sizeOfNanosDuration(value);
    }
  }

  /** Direct non-boxing coder for {@link EnumerationType} returning cached {@code Value}s. */
  private static final class EnumerationLogicalTypeCoder
      extends DirectLogicalTypeCoder<EnumerationType.Value> {
    private final EnumerationType enumType;

    EnumerationLogicalTypeCoder(EnumerationType enumType) {
      this.enumType = enumType;
    }

    @Override
    public void encode(EnumerationType.Value value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null EnumerationType.Value");
      }
      RowWireCodec.writeVarInt(value.getValue(), outStream);
    }

    @Override
    public EnumerationType.Value decode(InputStream inStream) throws IOException {
      return enumType.valueOf(RowWireCodec.readVarInt(inStream));
    }

    @Override
    protected long getEncodedElementByteSize(EnumerationType.Value value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null EnumerationType.Value");
      }
      return VarInt.getLength(value.getValue());
    }

    @Override
    @SuppressWarnings("EqualsGetClass")
    public boolean equals(@Nullable Object o) {
      return o instanceof EnumerationLogicalTypeCoder
          && Objects.equals(
              enumType.getValuesMap(), ((EnumerationLogicalTypeCoder) o).enumType.getValuesMap());
    }

    @Override
    public int hashCode() {
      return enumType.getValuesMap().hashCode();
    }
  }

  private static class LogicalTypeCoder<InputT, BaseT> extends Coder<InputT> {
    private final LogicalType<InputT, BaseT> logicalType;
    private final Coder<BaseT> baseTypeCoder;
    private final boolean isDateTime;
    private final boolean baseTypeAlwaysCheap;

    LogicalTypeCoder(LogicalType<InputT, BaseT> logicalType, Coder baseTypeCoder) {
      this.logicalType = logicalType;
      this.baseTypeCoder = baseTypeCoder;
      FieldType baseType = logicalType.getBaseType();
      this.isDateTime = baseType.getTypeName() == TypeName.DATETIME;
      this.baseTypeAlwaysCheap = isBaseTypeAlwaysCheap(baseType);
    }

    @Override
    public void encode(InputT value, OutputStream outStream) throws CoderException, IOException {
      BaseT baseType = logicalType.toBaseType(value);
      if (isDateTime) {
        baseType = (BaseT) ((ReadableInstant) baseType).toInstant();
      }
      baseTypeCoder.encode(baseType, outStream);
    }

    @Override
    public InputT decode(InputStream inStream) throws CoderException, IOException {
      BaseT baseType = baseTypeCoder.decode(inStream);
      return logicalType.toInputType(baseType);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      baseTypeCoder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      // we can't assume that InputT is consistent with equals.
      // TODO: We should plumb this through to logical types.
      return false;
    }

    @Override
    public Object structuralValue(InputT value) {
      if (baseTypeCoder.consistentWithEquals()) {
        return logicalType.toBaseType(value);
      } else {
        return baseTypeCoder.structuralValue(logicalType.toBaseType(value));
      }
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(InputT value) {
      if (baseTypeAlwaysCheap) {
        return true;
      }
      BaseT base = logicalType.toBaseType(value);
      if (isDateTime) {
        base = (BaseT) ((ReadableInstant) base).toInstant();
      }
      return baseTypeCoder.isRegisterByteSizeObserverCheap(base);
    }

    @Override
    public void registerByteSizeObserver(InputT value, ElementByteSizeObserver observer)
        throws Exception {
      BaseT base = logicalType.toBaseType(value);
      if (isDateTime) {
        base = (BaseT) ((ReadableInstant) base).toInstant();
      }
      baseTypeCoder.registerByteSizeObserver(base, observer);
    }
  }

  private static Coder<?> coderForLogicalType(LogicalType<?, ?> logicalType) {
    Class<?> clazz = logicalType.getClass();
    if (clazz == Date.class) {
      return DateLogicalTypeCoder.INSTANCE;
    }
    if (clazz == Time.class) {
      return TimeLogicalTypeCoder.INSTANCE;
    }
    if (clazz == DateTime.class && canUseDirectRowEncoding(DateTime.DATETIME_SCHEMA)) {
      return DateTimeLogicalTypeCoder.INSTANCE;
    }
    if (clazz == UuidLogicalType.class && canUseDirectRowEncoding(UuidLogicalType.UUID_SCHEMA)) {
      return UuidLogicalTypeCoder.INSTANCE;
    }
    if (clazz == Timestamp.class) {
      Schema baseSchema = logicalType.getBaseType().getRowSchema();
      if (baseSchema != null && canUseDirectRowEncoding(baseSchema)) {
        Integer precision = ((Timestamp) logicalType).getArgument();
        if (precision != null && precision >= 0 && precision <= 9) {
          return new TimestampLogicalTypeCoder(precision);
        }
      }
    }
    if (clazz == MicrosInstant.class) {
      Schema baseSchema = logicalType.getBaseType().getRowSchema();
      if (baseSchema != null && canUseDirectRowEncoding(baseSchema)) {
        return MicrosInstantLogicalTypeCoder.INSTANCE;
      }
    }
    if (clazz == NanosInstant.class) {
      Schema baseSchema = logicalType.getBaseType().getRowSchema();
      if (baseSchema != null && canUseDirectRowEncoding(baseSchema)) {
        return NanosInstantLogicalTypeCoder.INSTANCE;
      }
    }
    if (clazz == NanosDuration.class) {
      Schema baseSchema = logicalType.getBaseType().getRowSchema();
      if (baseSchema != null && canUseDirectRowEncoding(baseSchema)) {
        return NanosDurationLogicalTypeCoder.INSTANCE;
      }
    }
    if (clazz == EnumerationType.class) {
      return new EnumerationLogicalTypeCoder((EnumerationType) logicalType);
    }
    Coder<?> baseTypeCoder = coderForFieldType(logicalType.getBaseType());
    if (isIdentityPassThrough(logicalType)) {
      return baseTypeCoder;
    }
    return new LogicalTypeCoder(logicalType, baseTypeCoder);
  }

  /** Returns the coder used for a given primitive type. */
  public static <T> Coder<T> coderForFieldType(FieldType fieldType) {
    Coder<T> coder;
    switch (fieldType.getTypeName()) {
      case ROW:
        coder = (Coder<T>) SchemaCoder.of(fieldType.getRowSchema());
        break;
      case ARRAY:
        coder = (Coder<T>) ListCoder.of(coderForFieldType(fieldType.getCollectionElementType()));
        break;
      case ITERABLE:
        coder =
            (Coder<T>) IterableCoder.of(coderForFieldType(fieldType.getCollectionElementType()));
        break;
      case MAP:
        coder =
            (Coder<T>)
                MapCoder.of(
                    coderForFieldType(fieldType.getMapKeyType()),
                    coderForFieldType(fieldType.getMapValueType()));
        break;
      case LOGICAL_TYPE:
        coder = (Coder<T>) coderForLogicalType(fieldType.getLogicalType());
        break;
      default:
        coder = (Coder<T>) CODER_MAP.get(fieldType.getTypeName());
    }
    Preconditions.checkNotNull(coder, "Unexpected field type %s", fieldType.getTypeName());
    if (fieldType.getNullable()) {
      coder = NullableCoder.of(coder);
    }
    return coder;
  }
}
