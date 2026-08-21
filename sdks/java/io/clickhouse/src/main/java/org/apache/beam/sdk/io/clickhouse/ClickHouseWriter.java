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
package org.apache.beam.sdk.io.clickhouse;

import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** Writes Rows and field values using {@link ClickHousePipedOutputStream}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ClickHouseWriter {
  private static final Instant EPOCH_INSTANT = new Instant(0L);

  // 10^0 through 10^9 inclusive — precision is validated in [0, 9] by ColumnType.dateTime64.
  private static final long[] POW10 = {
    1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L
  };

  // 10^0 through 10^76 — the exclusive bound on a Decimal's unscaled value at each precision.
  // Precision is validated in [1, 76] by ColumnType.decimal.
  private static final BigInteger[] DECIMAL_BOUNDS = new BigInteger[77];

  static {
    for (int i = 0; i < DECIMAL_BOUNDS.length; i++) {
      DECIMAL_BOUNDS[i] = BigInteger.TEN.pow(i);
    }
  }

  /**
   * Truncates a value to a {@code Decimal(precision, scale)} column's scale and checks it against
   * the column's declared range.
   *
   * <p>Excess fractional digits are discarded toward zero, matching ClickHouse's own behavior and
   * the truncation {@link BinaryStreamUtils#writeDecimal} would otherwise apply. The result is then
   * bounded by the declared precision: {@code writeDecimal} only range-checks against the backing
   * storage width (32/64/128/256 bits, selected from the precision), which is wider than the
   * declared type, and ClickHouse's RowBinary reader does not re-check. Without this, a value such
   * as {@code 100000} would be stored in a {@code Decimal(5, 0)} column whose declared maximum is
   * {@code 99999}.
   */
  static BigDecimal truncateAndCheckDecimal(BigDecimal value, int precision, int scale) {
    BigDecimal truncated = value.setScale(scale, RoundingMode.DOWN);
    if (truncated.unscaledValue().abs().compareTo(DECIMAL_BOUNDS[precision]) >= 0) {
      throw new IllegalArgumentException(
          "value " + value + " is out of range for Decimal(" + precision + ", " + scale + ")");
    }
    return truncated;
  }

  /**
   * Encodes a timestamp into ClickHouse's {@code DateTime64(precision)} representation: a signed
   * 64-bit integer counting ticks of size 10<sup>-precision</sup> seconds since the Unix epoch.
   *
   * <p>Accepts either a Joda {@link ReadableInstant} (millisecond precision) or a {@link
   * java.time.Instant} (nanosecond precision). Sub-tick fractions are truncated toward negative
   * infinity, matching ClickHouse's own encoding for negative timestamps.
   */
  static long encodeDateTime64(Object value, int precision) {
    long epochSecond;
    int nanoOfSecond;
    if (value instanceof java.time.Instant) {
      java.time.Instant inst = (java.time.Instant) value;
      epochSecond = inst.getEpochSecond();
      nanoOfSecond = inst.getNano();
    } else if (value instanceof ReadableInstant) {
      long millis = ((ReadableInstant) value).getMillis();
      epochSecond = Math.floorDiv(millis, 1000L);
      nanoOfSecond = (int) Math.floorMod(millis, 1000L) * 1_000_000;
    } else {
      throw new IllegalArgumentException(
          "DateTime64 requires a Joda ReadableInstant or java.time.Instant, got "
              + (value == null ? "null" : value.getClass().getName()));
    }
    long subSecondTicks = nanoOfSecond / POW10[9 - precision];
    return Math.addExact(Math.multiplyExact(epochSecond, POW10[precision]), subSecondTicks);
  }

  @SuppressWarnings("unchecked")
  static void writeNullableValue(ClickHouseOutputStream stream, ColumnType columnType, Object value)
      throws IOException {

    if (value == null) {
      BinaryStreamUtils.writeNull(stream);
    } else {
      BinaryStreamUtils.writeNonNull(stream);
      writeValue(stream, columnType, value);
    }
  }

  @SuppressWarnings("unchecked")
  static void writeValue(ClickHouseOutputStream stream, ColumnType columnType, Object value)
      throws IOException {

    switch (columnType.typeName()) {
      case FIXEDSTRING:
        byte[] bytes;

        if (value instanceof String) {
          bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        } else {
          bytes = ((byte[]) value);
        }

        stream.writeBytes(bytes);
        break;

      case FLOAT32:
        BinaryStreamUtils.writeFloat32(stream, (Float) value);
        break;

      case FLOAT64:
        BinaryStreamUtils.writeFloat64(stream, (Double) value);
        break;

      case INT8:
        BinaryStreamUtils.writeInt8(stream, (Byte) value);
        break;

      case INT16:
        BinaryStreamUtils.writeInt16(stream, (Short) value);
        break;

      case INT32:
        BinaryStreamUtils.writeInt32(stream, (Integer) value);
        break;

      case INT64:
        BinaryStreamUtils.writeInt64(stream, (Long) value);
        break;

      case STRING:
        BinaryStreamUtils.writeString(stream, (String) value);
        break;

      case UINT8:
        BinaryStreamUtils.writeUnsignedInt8(stream, (Short) value);
        break;

      case UINT16:
        BinaryStreamUtils.writeUnsignedInt16(stream, (Integer) value);
        break;

      case UINT32:
        BinaryStreamUtils.writeUnsignedInt32(stream, (Long) value);
        break;

      case UINT64:
        BinaryStreamUtils.writeUnsignedInt64(stream, (Long) value);
        break;

      case ENUM8:
        Integer enum8 = columnType.enumValues().get((String) value);
        Preconditions.checkNotNull(
            enum8,
            "unknown enum value '" + value + "', possible values: " + columnType.enumValues());
        BinaryStreamUtils.writeInt8(stream, enum8);
        break;

      case ENUM16:
        Integer enum16 = columnType.enumValues().get((String) value);
        Preconditions.checkNotNull(
            enum16,
            "unknown enum value '" + value + "', possible values: " + columnType.enumValues());
        BinaryStreamUtils.writeInt16(stream, enum16);
        break;

      case DATE:
        Days epochDays = Days.daysBetween(EPOCH_INSTANT, (ReadableInstant) value);
        BinaryStreamUtils.writeUnsignedInt16(stream, epochDays.getDays());
        break;

      case DATETIME:
        long epochSeconds = ((ReadableInstant) value).getMillis() / 1000L;
        BinaryStreamUtils.writeUnsignedInt32(stream, epochSeconds);
        break;

      case DATETIME64:
        int precision =
            Preconditions.checkNotNull(
                columnType.precision(), "DateTime64 column is missing precision");
        BinaryStreamUtils.writeInt64(stream, encodeDateTime64(value, precision));
        break;

      case DECIMAL:
        int decimalPrecision =
            Preconditions.checkNotNull(columnType.precision(), "Decimal column missing precision");
        int decimalScale =
            Preconditions.checkNotNull(columnType.scale(), "Decimal column missing scale");
        // Truncate to the column scale and bound by the declared precision first; writeDecimal
        // then picks the 32/64/128/256-bit little-endian storage width from the precision and
        // writes the value as 10^-scale ticks.
        BigDecimal decimalValue =
            truncateAndCheckDecimal((BigDecimal) value, decimalPrecision, decimalScale);
        BinaryStreamUtils.writeDecimal(stream, decimalValue, decimalPrecision, decimalScale);
        break;

      case ARRAY:
        List<Object> values = (List<Object>) value;
        BinaryStreamUtils.writeVarInt(stream, values.size());
        for (Object arrayValue : values) {
          writeValue(stream, columnType.arrayElementType(), arrayValue);
        }
        break;
      case BOOL:
        BinaryStreamUtils.writeBoolean(stream, (Boolean) value);
        break;
      case TUPLE:
        RowWithStorage rowValues = (RowWithStorage) value;
        List<Object> tupleValues = rowValues.getValues();
        Collection<ColumnType> columnTypesList = columnType.tupleTypes().values();
        int index = 0;
        for (ColumnType ct : columnTypesList) {
          if (ct.nullable()) {
            writeNullableValue(stream, ct, tupleValues.get(index));
          } else {
            writeValue(stream, ct, tupleValues.get(index));
          }
          index++;
        }
        break;
    }
  }

  static void writeRow(ClickHouseOutputStream stream, TableSchema schema, Row row)
      throws IOException {
    for (TableSchema.Column column : schema.columns()) {
      if (!column.materializedOrAlias()) {
        Object value = row.getValue(column.name());

        if (column.columnType().nullable()) {
          writeNullableValue(stream, column.columnType(), value);
        } else {
          if (value == null) {
            value = column.defaultValue();
          }
          writeValue(stream, column.columnType(), value);
        }
      }
    }
  }
}
