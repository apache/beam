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
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
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

    List<SortField> fields = sortOrder.fields();
    String[] columnNames = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      columnNames[i] = icebergSchema.findColumnName(fields.get(i).sourceId());
    }

    // Create reusable ByteArrayOutputStreams for key and value encoding
    ByteArrayOutputStream keyBaos = new ByteArrayOutputStream();
    ByteArrayOutputStream valBaos = new ByteArrayOutputStream();

    try {
      for (Row row : rows) {
        keyBaos.reset();
        valBaos.reset();
        encodeSortKey(row, sortOrder, columnNames, keyBaos, icebergSchema, beamSchema);
        byte[] keyBytes = keyBaos.toByteArray();

        rowCoder.encode(row, valBaos);
        byte[] valBytes = valBaos.toByteArray();
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
  public static void encodeSortKey(
      Row row,
      SortOrder sortOrder,
      String[] columnNames,
      ByteArrayOutputStream baos,
      Schema icebergSchema,
      org.apache.beam.sdk.schemas.Schema beamSchema)
      throws IOException {

    List<SortField> fields = sortOrder.fields();

    for (int i = 0; i < fields.size(); i++) {
      SortField field = fields.get(i);
      String colName = columnNames[i];
      Object val = row.getValue(colName);

      if (!field.transform().isIdentity()) {
        Object icebergVal =
            IcebergUtils.beamValueToIcebergValue(icebergSchema.findType(field.sourceId()), val);
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
        prefixByte = nullsFirst ? (byte) 0x00 : (byte) 0xFF;
      } else {
        prefixByte = nullsFirst ? (byte) 0x01 : (byte) 0x00;
      }

      baos.write(prefixByte);

      if (!isNull) {
        writeValue(val, baos, isDesc);
      }
    }
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
  private static void writeValue(Object val, ByteArrayOutputStream baos, boolean invert)
      throws IOException {
    if (val instanceof String) {
      writeString((String) val, baos, invert);
    } else if (val instanceof Integer) {
      int v = (Integer) val;
      writeInt(v ^ Integer.MIN_VALUE, baos, invert);
    } else if (val instanceof Long) {
      long v = (Long) val;
      writeLong(v ^ Long.MIN_VALUE, baos, invert);
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
    } else if (val instanceof byte[]) {
      writeByteArray((byte[]) val, baos, invert);
    } else if (val instanceof ByteBuffer) {
      writeByteArray(((ByteBuffer) val).array(), baos, invert);
    } else if (val instanceof ReadableInstant) {
      long enc = ((ReadableInstant) val).getMillis() ^ Long.MIN_VALUE;
      writeLong(enc, baos, invert);
    } else if (val instanceof Instant) {
      long enc = ((Instant) val).toEpochMilli() ^ Long.MIN_VALUE;
      writeLong(enc, baos, invert);
    } else if (val instanceof Date) {
      long enc = ((Date) val).getTime() ^ Long.MIN_VALUE;
      writeLong(enc, baos, invert);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported type for sorting: " + val.getClass().getName());
    }
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
