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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableInstant;

/**
 * A utility class to sort Beam {@link Row}s based on an Iceberg {@link SortOrder}. Leverages {@link
 * BufferedExternalSorter} to spill to local disk when elements exceed memory limit.
 */
class IcebergRowSorter implements Serializable {

  public static Iterable<Row> sortRows(
      Iterable<Row> rows,
      SortOrder sortOrder,
      Schema icebergSchema,
      org.apache.beam.sdk.schemas.Schema beamSchema) {

    if (sortOrder == null || !sortOrder.isSorted()) {
      return rows;
    }

    BufferedExternalSorter.Options sorterOptions = BufferedExternalSorter.options();
    BufferedExternalSorter sorter = BufferedExternalSorter.create(sorterOptions);
    RowCoder rowCoder = RowCoder.of(beamSchema);

    try {
      for (Row row : rows) {
        byte[] keyBytes = encodeSortKey(row, sortOrder, icebergSchema, beamSchema);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        rowCoder.encode(row, baos);
        byte[] valBytes = baos.toByteArray();
        sorter.add(KV.of(keyBytes, valBytes));
      }

      Iterable<KV<byte[], byte[]>> sortedKVs = sorter.sort();
      return new Iterable<Row>() {
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
                ByteArrayInputStream bais = new ByteArrayInputStream(next.getValue());
                return rowCoder.decode(bais);
              } catch (IOException e) {
                throw new RuntimeException("Failed to decode Row during sorting", e);
              }
            }
          };
        }
      };

    } catch (IOException e) {
      throw new RuntimeException("Failed to sort rows with external sorter", e);
    }
  }

  @SuppressWarnings("nullness")
  public static byte[] encodeSortKey(
      Row row,
      SortOrder sortOrder,
      Schema icebergSchema,
      org.apache.beam.sdk.schemas.Schema beamSchema)
      throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    for (SortField field : sortOrder.fields()) {
      String colName = icebergSchema.findColumnName(field.sourceId());
      Object val = row.getValue(colName);

      if (!field.transform().isIdentity()) {
        Object icebergVal =
            IcebergUtils.beamRowToIcebergRecord(icebergSchema, row).getField(colName);
        if (icebergVal != null) {
          val = field.transform().apply(icebergVal);
        } else {
          val = null;
        }
      }

      boolean isNull = (val == null);
      boolean isDesc = (field.direction() == SortDirection.DESC);
      boolean nullsFirst = (field.nullOrder() == NullOrder.NULLS_FIRST);

      // Determine correct header prefix to fulfill the NullOrder contracts
      byte prefixByte;
      if (isNull) {
        if (isDesc) {
          // Descending: High byte keys sort first.
          // If Nulls First -> Null gets highest byte (0xFF)
          // If Nulls Last -> Null gets lowest byte (0x00)
          prefixByte = nullsFirst ? (byte) 0xFF : (byte) 0x00;
        } else {
          // Ascending: Low byte keys sort first.
          // If Nulls First -> Null gets lowest byte (0x00)
          // If Nulls Last -> Null gets highest byte (0xFF)
          prefixByte = nullsFirst ? (byte) 0x00 : (byte) 0xFF;
        }
      } else {
        if (isDesc) {
          // If non-null and Descending, use a neutral value that sits opposite to the null byte
          prefixByte = nullsFirst ? (byte) 0xFE : (byte) 0x01;
        } else {
          prefixByte = nullsFirst ? (byte) 0x01 : (byte) 0x00;
        }
      }

      baos.write(prefixByte);

      if (!isNull) {
        byte[] valBytes = encodeValue(val);
        // Bitwise invert non-null bytes to sort descending lexicographically
        if (isDesc) {
          for (int i = 0; i < valBytes.length; i++) {
            valBytes[i] = (byte) ~valBytes[i];
          }
        }
        baos.write(valBytes);
      }
    }

    return baos.toByteArray();
  }

  @SuppressWarnings("JavaUtilDate")
  private static byte[] encodeValue(@Nullable Object val) throws IOException {
    if (val == null) {
      return new byte[0];
    }
    if (val instanceof String) {
      return encodeString((String) val);
    } else if (val instanceof Integer) {
      int v = (Integer) val;
      return ByteBuffer.allocate(4).putInt(v ^ Integer.MIN_VALUE).array();
    } else if (val instanceof Long) {
      long v = (Long) val;
      return ByteBuffer.allocate(8).putLong(v ^ Long.MIN_VALUE).array();
    } else if (val instanceof Float) {
      int bits = Float.floatToIntBits((Float) val);
      bits = (bits >= 0) ? (bits ^ Integer.MIN_VALUE) : ~bits;
      return ByteBuffer.allocate(4).putInt(bits).array();
    } else if (val instanceof Double) {
      long bits = Double.doubleToLongBits((Double) val);
      bits = (bits >= 0) ? (bits ^ Long.MIN_VALUE) : ~bits;
      return ByteBuffer.allocate(8).putLong(bits).array();
    } else if (val instanceof Boolean) {
      return new byte[] {((Boolean) val) ? (byte) 0x01 : (byte) 0x00};
    } else if (val instanceof byte[]) {
      return encodeByteArray((byte[]) val);
    } else if (val instanceof ByteBuffer) {
      return encodeByteArray(((ByteBuffer) val).array());
    } else if (val instanceof ReadableInstant) {
      long enc = ((ReadableInstant) val).getMillis() ^ Long.MIN_VALUE;
      return ByteBuffer.allocate(8).putLong(enc).array();
    } else if (val instanceof Instant) {
      long enc = ((Instant) val).toEpochMilli() ^ Long.MIN_VALUE;
      return ByteBuffer.allocate(8).putLong(enc).array();
    } else if (val instanceof Date) {
      long enc = ((Date) val).getTime() ^ Long.MIN_VALUE;
      return ByteBuffer.allocate(8).putLong(enc).array();
    }

    return encodeString(val.toString());
  }

  private static byte[] encodeString(String s) throws IOException {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    return encodeByteArray(bytes);
  }

  /**
   * Escape protocol to cleanly prevent collisions. Maps 0x00 -> [0x01, 0x01] Maps 0x01 -> [0x01,
   * 0x02] Safely terminates sequence with 0x00.
   */
  private static byte[] encodeByteArray(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length + 2);
    for (byte b : bytes) {
      if (b == 0x00) {
        baos.write(0x01);
        baos.write(0x01);
      } else if (b == 0x01) {
        baos.write(0x01);
        baos.write(0x02);
      } else {
        baos.write(b);
      }
    }
    baos.write(0x00); // Safe boundary delimiter
    return baos.toByteArray();
  }
}
