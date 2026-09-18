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
package org.apache.beam.sdk.io.iceberg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to sort Beam {@link Row}s based on an Iceberg {@link SortOrder}. Leverages {@link
 * BufferedExternalSorter} to spill to local disk when elements exceed memory limit.
 */
class IcebergRowSorter {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRowSorter.class);

  /** Width of an encoded decimal key. 16 bytes covers Iceberg's maximum precision of 38 digits. */
  private static final int DECIMAL_KEY_BYTES = 16;

  /**
   * Best-effort cleanup of a temporary spill directory. Failures are logged rather than propagated.
   */
  private static void deleteTempDir(@Nullable Path tempDir) {
    if (tempDir == null) {
      return;
    }
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(tempDir)) {
      for (Path entry : entries) {
        if (!Files.deleteIfExists(entry)) {
          LOG.debug("Temporary sort file {} was already removed", entry);
        }
      }
    } catch (IOException | RuntimeException e) {
      LOG.debug("Failed to list temporary sort directory {}", tempDir, e);
      return;
    }
    try {
      if (!Files.deleteIfExists(tempDir)) {
        LOG.debug("Temporary sort directory {} was already removed", tempDir);
      }
    } catch (IOException | RuntimeException e) {
      LOG.debug("Failed to delete temporary sort directory {}", tempDir, e);
    }
  }

  /**
   * Sorts {@code rows} according to {@code sortOrder}, spilling to local disk beyond the in-memory
   * buffer.
   *
   * <p>The caller <b>must</b> close the returned iterable (ideally via try-with-resources). Closing
   * reclaims the temporary spill directory; skipping it leaks that directory on the worker's local
   * disk, which matters on long-lived runner harnesses that process many bundles.
   *
   * <p>Note that spilled rows are written unencrypted to the worker's local disk, in a directory
   * created with owner-only permissions.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static CloseableIterable<Row> sortRows(
      Iterable<Row> rows,
      SortOrder sortOrder,
      Schema icebergSchema,
      org.apache.beam.sdk.schemas.Schema beamSchema) {

    if (sortOrder == null || !sortOrder.isSorted()) {
      return CloseableIterable.withNoopClose(rows);
    }

    final Path sortTempDir;
    try {
      sortTempDir = Files.createTempDirectory("iceberg-row-sorter-");
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary directory for IcebergRowSorter", e);
    }

    BufferedExternalSorter.Options sorterOptions =
        BufferedExternalSorter.options()
            .withTempLocation(sortTempDir.toString())
            .withExternalSorterType(ExternalSorter.Options.SorterType.NATIVE);
    BufferedExternalSorter sorter = BufferedExternalSorter.create(sorterOptions);
    RowCoder rowCoder = RowCoder.of(beamSchema);

    List<SortField> fields = sortOrder.fields();
    String[] columnNames = new String[fields.size()];
    Type[] sourceTypes = new Type[fields.size()];
    Type[] resultTypes = new Type[fields.size()];
    @Nullable SerializableFunction<Object, Object>[] boundTransforms =
        new SerializableFunction[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      SortField field = fields.get(i);
      columnNames[i] = icebergSchema.findColumnName(field.sourceId());
      Type sourceType = icebergSchema.findType(field.sourceId());
      sourceTypes[i] = sourceType;
      if (field.transform().isIdentity()) {
        resultTypes[i] = sourceType;
        boundTransforms[i] = null;
      } else {
        Transform<Object, Object> transform = (Transform<Object, Object>) field.transform();
        resultTypes[i] = transform.getResultType(sourceType);
        boundTransforms[i] = transform.bind(sourceType);
      }
    }

    // Create reusable ByteArrayOutputStreams for key and value encoding
    ByteArrayOutputStream keyBaos = new ByteArrayOutputStream();
    ByteArrayOutputStream valBaos = new ByteArrayOutputStream();

    try {
      for (Row row : rows) {
        keyBaos.reset();
        valBaos.reset();
        encodeSortKey(
            row, sortOrder, columnNames, sourceTypes, resultTypes, boundTransforms, keyBaos);
        byte[] keyBytes = keyBaos.toByteArray();

        rowCoder.encode(row, valBaos);
        byte[] valBytes = valBaos.toByteArray();
        sorter.add(KV.of(keyBytes, valBytes));
      }

      Iterable<KV<byte[], byte[]>> sortedKVs = sorter.sort();
      Iterable<Row> decodedRows =
          new Iterable<Row>() {
            @Override
            public Iterator<Row> iterator() {
              final Iterator<KV<byte[], byte[]>> it = sortedKVs.iterator();
              return new Iterator<Row>() {
                @Override
                public boolean hasNext() {
                  return it.hasNext();
                }

                @Override
                public Row next() {
                  KV<byte[], byte[]> next = it.next();
                  try {
                    return rowCoder.decode(new ByteArrayInputStream(next.getValue()));
                  } catch (IOException e) {
                    deleteTempDir(sortTempDir);
                    throw new RuntimeException("Failed to decode Row during sorting", e);
                  }
                }
              };
            }
          };

      return CloseableIterable.combine(decodedRows, () -> deleteTempDir(sortTempDir));

    } catch (IOException | RuntimeException e) {
      deleteTempDir(sortTempDir);
      throw new RuntimeException("Failed to sort rows with external sorter", e);
    }
  }

  @SuppressWarnings({"nullness", "unchecked", "rawtypes"})
  public static void encodeSortKey(
      Row row,
      SortOrder sortOrder,
      String[] columnNames,
      ByteArrayOutputStream baos,
      Schema icebergSchema,
      org.apache.beam.sdk.schemas.Schema beamSchema)
      throws IOException {

    List<SortField> fields = sortOrder.fields();
    Type[] sourceTypes = new Type[fields.size()];
    Type[] resultTypes = new Type[fields.size()];
    @Nullable SerializableFunction<Object, Object>[] boundTransforms =
        new SerializableFunction[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      SortField field = fields.get(i);
      Type sourceType = icebergSchema.findType(field.sourceId());
      sourceTypes[i] = sourceType;
      if (field.transform().isIdentity()) {
        resultTypes[i] = sourceType;
        boundTransforms[i] = null;
      } else {
        Transform<Object, Object> transform = (Transform<Object, Object>) field.transform();
        resultTypes[i] = transform.getResultType(sourceType);
        boundTransforms[i] = transform.bind(sourceType);
      }
    }
    encodeSortKey(row, sortOrder, columnNames, sourceTypes, resultTypes, boundTransforms, baos);
  }

  @SuppressWarnings("nullness")
  private static void encodeSortKey(
      Row row,
      SortOrder sortOrder,
      String[] columnNames,
      Type[] sourceTypes,
      Type[] resultTypes,
      @Nullable SerializableFunction<Object, Object>[] boundTransforms,
      ByteArrayOutputStream baos)
      throws IOException {

    List<SortField> fields = sortOrder.fields();

    for (int i = 0; i < fields.size(); i++) {
      SortField field = fields.get(i);
      String colName = columnNames[i];

      // Normalize into the Iceberg value domain up front so that the encoder only ever deals with
      // Iceberg's representation, regardless of which Beam logical type the row carried.
      Type sourceType = sourceTypes[i];
      Type valueType = resultTypes[i];
      Object val = IcebergUtils.beamValueToIcebergValue(sourceType, row.getValue(colName));

      SerializableFunction<Object, Object> boundTransform = boundTransforms[i];
      if (boundTransform != null && val != null) {
        val = boundTransform.apply(toTransformInput(sourceType, val));
      }

      boolean isNull = (val == null);
      boolean isDesc = (field.direction() == SortDirection.DESC);
      boolean nullsFirst = (field.nullOrder() == NullOrder.NULLS_FIRST);

      // Determine correct header prefix to fulfill the NullOrder contracts
      byte prefixByte;
      if (isNull) {
        prefixByte = nullsFirst ? (byte) 0x00 : (byte) 0xFF;
      } else {
        prefixByte = nullsFirst ? (byte) 0x01 : (byte) 0x00;
      }

      baos.write(prefixByte);

      if (!isNull) {
        writeValue(val, valueType, baos, isDesc);
      }
    }
  }

  /**
   * Converts a normalized Iceberg value into the internal representation expected by bound Iceberg
   * {@link Transform} functions (e.g. epoch days as {@code Integer} for {@code DATE}, epoch micros
   * as {@code Long} for {@code TIME} / {@code TIMESTAMP}).
   */
  @SuppressWarnings("JavaUtilDate")
  private static Object toTransformInput(Type sourceType, Object val) {
    if (val instanceof LocalDate) {
      return DateTimeUtil.daysFromDate((LocalDate) val);
    } else if (val instanceof LocalTime) {
      return DateTimeUtil.microsFromTime((LocalTime) val);
    } else if (val instanceof LocalDateTime) {
      return DateTimeUtil.microsFromTimestamp((LocalDateTime) val);
    } else if (val instanceof OffsetDateTime) {
      return DateTimeUtil.microsFromTimestamptz((OffsetDateTime) val);
    } else if (val instanceof ReadableInstant) {
      return ((ReadableInstant) val).getMillis() * 1000L;
    } else if (val instanceof Instant) {
      return DateTimeUtil.microsFromInstant((Instant) val);
    } else if (val instanceof Date) {
      return ((Date) val).getTime() * 1000L;
    } else if (val instanceof Short || val instanceof Byte) {
      return ((Number) val).intValue();
    } else if (val instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) val);
    } else if (val instanceof BigDecimal && sourceType instanceof Types.DecimalType) {
      int scale = ((Types.DecimalType) sourceType).scale();
      return ((BigDecimal) val).setScale(scale, RoundingMode.UNNECESSARY);
    }
    return val;
  }

  private static void writeInt(int v, ByteArrayOutputStream baos, boolean invert) {
    byte b3 = (byte) (v >>> 24);
    byte b2 = (byte) (v >>> 16);
    byte b1 = (byte) (v >>> 8);
    byte b0 = (byte) v;
    if (invert) {
      baos.write(~b3);
      baos.write(~b2);
      baos.write(~b1);
      baos.write(~b0);
    } else {
      baos.write(b3);
      baos.write(b2);
      baos.write(b1);
      baos.write(b0);
    }
  }

  private static void writeLong(long v, ByteArrayOutputStream baos, boolean invert) {
    byte b7 = (byte) (v >>> 56);
    byte b6 = (byte) (v >>> 48);
    byte b5 = (byte) (v >>> 40);
    byte b4 = (byte) (v >>> 32);
    byte b3 = (byte) (v >>> 24);
    byte b2 = (byte) (v >>> 16);
    byte b1 = (byte) (v >>> 8);
    byte b0 = (byte) v;
    if (invert) {
      baos.write(~b7);
      baos.write(~b6);
      baos.write(~b5);
      baos.write(~b4);
      baos.write(~b3);
      baos.write(~b2);
      baos.write(~b1);
      baos.write(~b0);
    } else {
      baos.write(b7);
      baos.write(b6);
      baos.write(b5);
      baos.write(b4);
      baos.write(b3);
      baos.write(b2);
      baos.write(b1);
      baos.write(b0);
    }
  }

  @SuppressWarnings("JavaUtilDate")
  private static void writeValue(
      Object val, @Nullable Type type, ByteArrayOutputStream baos, boolean invert)
      throws IOException {
    if (val instanceof CharSequence) {
      writeString(val.toString(), baos, invert);
    } else if (val instanceof Integer || val instanceof Short || val instanceof Byte) {
      writeInt(((Number) val).intValue() ^ Integer.MIN_VALUE, baos, invert);
    } else if (val instanceof Long) {
      writeLong((Long) val ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof Float) {
      int bits = Float.floatToIntBits((Float) val);
      bits = (bits >= 0) ? (bits ^ Integer.MIN_VALUE) : ~bits;
      writeInt(bits, baos, invert);
    } else if (val instanceof Double) {
      long bits = Double.doubleToLongBits((Double) val);
      bits = (bits >= 0) ? (bits ^ Long.MIN_VALUE) : ~bits;
      writeLong(bits, baos, invert);
    } else if (val instanceof Boolean) {
      byte b = ((Boolean) val) ? (byte) 0x01 : (byte) 0x00;
      baos.write(invert ? ~b : b);
    } else if (val instanceof BigDecimal) {
      writeDecimal((BigDecimal) val, type, baos, invert);
    } else if (val instanceof byte[]) {
      writeByteArray((byte[]) val, baos, invert);
    } else if (val instanceof ByteBuffer) {
      writeByteArray(toByteArray((ByteBuffer) val), baos, invert);
    } else if (val instanceof LocalDate) {
      writeLong(((LocalDate) val).toEpochDay() ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof LocalTime) {
      writeLong(((LocalTime) val).toNanoOfDay() ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof LocalDateTime) {
      writeLong(
          DateTimeUtil.microsFromTimestamp((LocalDateTime) val) ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof OffsetDateTime) {
      writeLong(
          DateTimeUtil.microsFromTimestamptz((OffsetDateTime) val) ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof ReadableInstant) {
      writeLong(((ReadableInstant) val).getMillis() ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof Instant) {
      writeLong(((Instant) val).toEpochMilli() ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof Date) {
      writeLong(((Date) val).getTime() ^ Long.MIN_VALUE, baos, invert);
    } else if (val instanceof UUID) {
      // Iceberg orders UUIDs by their 16-byte big-endian representation, so flip the sign bit of
      // each half to turn Java's signed longs into an unsigned lexicographic ordering.
      UUID uuid = (UUID) val;
      writeLong(uuid.getMostSignificantBits() ^ Long.MIN_VALUE, baos, invert);
      writeLong(uuid.getLeastSignificantBits() ^ Long.MIN_VALUE, baos, invert);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported type for sorting: " + val.getClass().getName());
    }
  }

  /**
   * Encodes a decimal as a fixed-width, sign-flipped two's complement big-endian integer.
   *
   * <p>Iceberg pins the scale of a decimal column in the schema, so rescaling to that scale makes
   * every value in the column directly comparable as a fixed-width unscaled integer. A fixed width
   * is required because this encoding carries no terminator; {@value #DECIMAL_KEY_BYTES} bytes
   * covers Iceberg's maximum precision of 38 digits.
   */
  private static void writeDecimal(
      BigDecimal val, @Nullable Type type, ByteArrayOutputStream baos, boolean invert) {
    int scale =
        (type instanceof Types.DecimalType) ? ((Types.DecimalType) type).scale() : val.scale();
    BigInteger unscaled;
    try {
      unscaled = val.setScale(scale, RoundingMode.UNNECESSARY).unscaledValue();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format(
              "Decimal sort value %s does not fit the column's declared scale of %d.", val, scale),
          e);
    }

    byte[] magnitude = unscaled.toByteArray();
    if (magnitude.length > DECIMAL_KEY_BYTES) {
      throw new IllegalArgumentException(
          String.format("Decimal sort value %s exceeds Iceberg's maximum decimal precision.", val));
    }

    byte[] encoded = new byte[DECIMAL_KEY_BYTES];
    // Sign-extend so that shorter negative magnitudes stay ordered against longer ones.
    Arrays.fill(
        encoded,
        0,
        DECIMAL_KEY_BYTES - magnitude.length,
        (byte) (unscaled.signum() < 0 ? 0xFF : 0x00));
    System.arraycopy(magnitude, 0, encoded, DECIMAL_KEY_BYTES - magnitude.length, magnitude.length);
    // Flip the sign bit so two's complement sorts correctly as unsigned bytes.
    encoded[0] ^= (byte) 0x80;

    for (byte b : encoded) {
      baos.write(invert ? ~b : b);
    }
  }

  /** Copies the readable region of a buffer without assuming an accessible backing array. */
  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  private static void writeString(String s, ByteArrayOutputStream baos, boolean invert)
      throws IOException {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    writeByteArray(bytes, baos, invert);
  }

  private static void writeByteArray(byte[] bytes, ByteArrayOutputStream baos, boolean invert) {
    for (byte b : bytes) {
      if (b == 0x00) {
        baos.write(invert ? ~(byte) 0x01 : (byte) 0x01);
        baos.write(invert ? ~(byte) 0x01 : (byte) 0x01);
      } else if (b == 0x01) {
        baos.write(invert ? ~(byte) 0x01 : (byte) 0x01);
        baos.write(invert ? ~(byte) 0x02 : (byte) 0x02);
      } else {
        baos.write(invert ? ~b : b);
      }
    }
    baos.write(invert ? ~(byte) 0x00 : (byte) 0x00);
  }
}
