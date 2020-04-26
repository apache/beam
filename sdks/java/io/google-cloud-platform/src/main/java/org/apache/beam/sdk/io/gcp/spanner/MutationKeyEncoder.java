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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.isPointDelete;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema.KeyPart;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given the Schema, Encodes the table name and Key into a lexicographically sortable {@code
 * byte[]}.
 */
class MutationKeyEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(MutationKeyEncoder.class);
  private static final int ROWS_PER_UNKNOWN_TABLE_LOG_MESSAGE = 10000;
  private static final DateTime MIN_DATE = new DateTime(1, 1, 1, 0, 0);
  private final SpannerSchema schema;

  // Global single instance. Use Concurrent Map and AtomicInteger for thread-safety.
  @VisibleForTesting
  private static final Map<String, AtomicInteger> unknownTablesWarnings = new ConcurrentHashMap<>();

  public MutationKeyEncoder(SpannerSchema schema) {
    this.schema = schema;
  }

  /**
   * Builds a lexicographically sortable binary key based on a primary key descriptor.
   *
   * @param m a spanner mutation.
   * @return a binary string that preserves the ordering of the primary key.
   */
  public byte[] encodeTableNameAndKey(Mutation m) {
    OrderedCode orderedCode = new OrderedCode();
    String tableName = m.getTable().toLowerCase();

    if (schema.getColumns(tableName).isEmpty()) {
      // Log an warning for an unknown table.
      if (!unknownTablesWarnings.containsKey(tableName)) {
        unknownTablesWarnings.putIfAbsent(tableName, new AtomicInteger(0));
      }
      // Only log every 10000 rows per table, or there will be way too much logging...
      int numWarnings = unknownTablesWarnings.get(tableName).incrementAndGet();
      if (1 == (numWarnings % ROWS_PER_UNKNOWN_TABLE_LOG_MESSAGE)) {
        System.err.printf(
            "Performance issue: Mutation references an unknown table: %s. "
                + "See SpannerIO documentation section 'Database Schema Preparation' "
                + "(At least %,d occurrences)\n",
            tableName, numWarnings);
      }
    }

    orderedCode.writeBytes(tableName.getBytes(StandardCharsets.UTF_8));

    if (m.getOperation() == Op.DELETE) {
      if (isPointDelete(m)) {
        Key key = m.getKeySet().getKeys().iterator().next();
        encodeKey(orderedCode, tableName, key);
      } else {
        // The key is left empty for non-point deletes, since there is no general way to batch them.
      }
    } else {
      encodeKey(orderedCode, m);
    }
    return orderedCode.getEncodedBytes();
  }

  private void encodeKey(OrderedCode orderedCode, Mutation m) {
    Map<String, Value> mutationMap = mutationAsMap(m);
    for (SpannerSchema.KeyPart part : schema.getKeyParts(m.getTable())) {
      Value val = mutationMap.get(part.getField());
      if (val == null || val.isNull()) {
        if (part.isDesc()) {
          orderedCode.writeInfinityDecreasing();
        } else {
          orderedCode.writeInfinity();
        }
      } else {
        Type.Code code = val.getType().getCode();
        switch (code) {
          case BOOL:
            writeNumber(orderedCode, part, (long) (val.getBool() ? 0 : 1));
            break;
          case INT64:
            writeNumber(orderedCode, part, val.getInt64());
            break;
          case FLOAT64:
            writeNumber(orderedCode, part, Double.doubleToLongBits(val.getFloat64()));
            break;
          case STRING:
            writeString(orderedCode, part, val.getString());
            break;
          case BYTES:
            writeBytes(orderedCode, part, val.getBytes());
            break;
          case TIMESTAMP:
            writeTimestamp(orderedCode, part, val.getTimestamp());
            break;
          case DATE:
            writeNumber(orderedCode, part, encodeDate(val.getDate()));
            break;
          default:
            throw new IllegalArgumentException("Unknown type " + val.getType());
        }
      }
    }
  }

  private void encodeKey(OrderedCode orderedCode, String tableName, Key key) {
    List<SpannerSchema.KeyPart> parts = schema.getKeyParts(tableName);
    Iterator<Object> it = key.getParts().iterator();
    for (SpannerSchema.KeyPart part : parts) {
      Object value = it.next();
      if (value == null) {
        if (part.isDesc()) {
          orderedCode.writeInfinityDecreasing();
        } else {
          orderedCode.writeInfinity();
        }
      } else {
        if (value instanceof Boolean) {
          writeNumber(orderedCode, part, (long) ((Boolean) value ? 0 : 1));
        } else if (value instanceof Long) {
          writeNumber(orderedCode, part, (long) value);
        } else if (value instanceof Double) {
          writeNumber(orderedCode, part, Double.doubleToLongBits((double) value));
        } else if (value instanceof String) {
          writeString(orderedCode, part, (String) value);
        } else if (value instanceof ByteArray) {
          writeBytes(orderedCode, part, (ByteArray) value);
        } else if (value instanceof Timestamp) {
          writeTimestamp(orderedCode, part, (Timestamp) value);
        } else if (value instanceof Date) {
          writeNumber(orderedCode, part, encodeDate((Date) value));
        } else {
          throw new IllegalArgumentException("Unknown key part " + value);
        }
      }
    }
  }

  private void writeBytes(OrderedCode orderedCode, KeyPart part, ByteArray bytes) {
    if (part.isDesc()) {
      orderedCode.writeBytesDecreasing(bytes.toByteArray());
    } else {
      orderedCode.writeBytes(bytes.toByteArray());
    }
  }

  private void writeNumber(OrderedCode orderedCode, KeyPart part, long v) {
    if (part.isDesc()) {
      orderedCode.writeSignedNumDecreasing(v);
    } else {
      orderedCode.writeSignedNumIncreasing(v);
    }
  }

  private void writeString(OrderedCode orderedCode, KeyPart part, String v) {
    if (part.isDesc()) {
      orderedCode.writeBytesDecreasing(v.getBytes(StandardCharsets.UTF_8));
    } else {
      orderedCode.writeBytes(v.getBytes(StandardCharsets.UTF_8));
    }
  }

  private void writeTimestamp(OrderedCode orderedCode, KeyPart part, Timestamp v) {
    if (part.isDesc()) {
      orderedCode.writeNumDecreasing(v.getSeconds());
      orderedCode.writeNumDecreasing(v.getNanos());
    } else {
      orderedCode.writeNumIncreasing(v.getSeconds());
      orderedCode.writeNumIncreasing(v.getNanos());
    }
  }

  private static Map<String, Value> mutationAsMap(Mutation m) {
    Map<String, Value> result = new HashMap<>();
    Iterator<String> coli = m.getColumns().iterator();
    Iterator<Value> vali = m.getValues().iterator();
    while (coli.hasNext()) {
      String column = coli.next();
      Value val = vali.next();
      result.put(column.toLowerCase(), val);
    }
    return result;
  }

  private static int encodeDate(Date date) {

    MutableDateTime jodaDate = new MutableDateTime();
    jodaDate.setDate(date.getYear(), date.getMonth(), date.getDayOfMonth());

    return Days.daysBetween(MIN_DATE, jodaDate).getDays();
  }

  // Give tests access to the Unknown Tables Warning count.
  @VisibleForTesting
  static Map<String, AtomicInteger> getUnknownTablesWarningsMap() {
    return unknownTablesWarnings;
  }
}
