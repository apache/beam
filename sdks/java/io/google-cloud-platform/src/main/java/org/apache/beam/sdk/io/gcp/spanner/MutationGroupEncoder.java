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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

/** Given the Spanner Schema, efficiently encodes the mutation group. */
class MutationGroupEncoder {

  private static final SerializableCoder<MutationGroup> CODER =
      SerializableCoder.of(MutationGroup.class);
  private static final DateTime MIN_DATE = new DateTime(1, 1, 1, 0, 0);
  private final SpannerSchema schema;

  public MutationGroupEncoder(SpannerSchema schema) {
    this.schema = schema;
  }

  public byte[] encode(MutationGroup g) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      CODER.encode(g, bos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }

  public MutationGroup decode(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

    try {
      return CODER.decode(bis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds a lexicographically sortable binary key based on a primary key descriptor.
   *
   * @param m a spanner mutation.
   * @return a binary string that preserves the ordering of the primary key.
   */
  public byte[] encodeKey(Mutation m) {
    Map<String, Value> mutationMap = mutationAsMap(m);
    OrderedCode orderedCode = new OrderedCode();
    for (SpannerSchema.KeyPart part : schema.getKeyParts(m.getTable())) {
      Value val = mutationMap.get(part.getField());
      if (val.isNull()) {
        if (part.isDesc()) {
          orderedCode.writeInfinityDecreasing();
        } else {
          orderedCode.writeInfinity();
        }
      } else {
        Type.Code code = val.getType().getCode();
        switch (code) {
          case BOOL:
            long v = val.getBool() ? 0 : 1;
            if (part.isDesc()) {
              orderedCode.writeSignedNumDecreasing(v);
            } else {
              orderedCode.writeSignedNumIncreasing(v);
            }
            break;
          case INT64:
            if (part.isDesc()) {
              orderedCode.writeSignedNumDecreasing(val.getInt64());
            } else {
              orderedCode.writeSignedNumIncreasing(val.getInt64());
            }
            break;
          case FLOAT64:
            if (part.isDesc()) {
              orderedCode.writeSignedNumDecreasing(Double.doubleToLongBits(val.getFloat64()));
            } else {
              orderedCode.writeSignedNumIncreasing(Double.doubleToLongBits(val.getFloat64()));
            }
            break;
          case STRING:
            if (part.isDesc()) {
              orderedCode.writeBytesDecreasing(val.getString().getBytes(StandardCharsets.UTF_8));
            } else {
              orderedCode.writeBytes(val.getString().getBytes(StandardCharsets.UTF_8));
            }
            break;
          case BYTES:
            if (part.isDesc()) {
              orderedCode.writeBytesDecreasing(val.getBytes().toByteArray());
            } else {
              orderedCode.writeBytes(val.getBytes().toByteArray());
            }
            break;
          case TIMESTAMP:
            {
              Timestamp value = val.getTimestamp();
              if (part.isDesc()) {
                orderedCode.writeNumDecreasing(value.getSeconds());
                orderedCode.writeNumDecreasing(value.getNanos());
              } else {
                orderedCode.writeNumIncreasing(value.getSeconds());
                orderedCode.writeNumIncreasing(value.getNanos());
              }
              break;
            }
          case DATE:
            Date value = val.getDate();
            if (part.isDesc()) {
              orderedCode.writeSignedNumDecreasing(encodeDate(value));
            } else {
              orderedCode.writeSignedNumIncreasing(encodeDate(value));
            }
            break;
          default:
            throw new IllegalArgumentException("Unknown type " + val.getType());
        }
      }
    }
    return orderedCode.getEncodedBytes();
  }

  public byte[] encodeKey(String table, Key key) {
    OrderedCode orderedCode = new OrderedCode();
    List<SpannerSchema.KeyPart> parts = schema.getKeyParts(table);
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
          long v = (Boolean) value ? 0 : 1;
          if (part.isDesc()) {
            orderedCode.writeSignedNumDecreasing(v);
          } else {
            orderedCode.writeSignedNumIncreasing(v);
          }
        } else if (value instanceof Long) {
          long v = (long) value;
          if (part.isDesc()) {
            orderedCode.writeSignedNumDecreasing(v);
          } else {
            orderedCode.writeSignedNumIncreasing(v);
          }
        } else if (value instanceof Double) {
          long v = Double.doubleToLongBits((double) value);
          if (part.isDesc()) {
            orderedCode.writeSignedNumDecreasing(v);
          } else {
            orderedCode.writeSignedNumIncreasing(v);
          }
        } else if (value instanceof String) {
          String v = (String) value;
          if (part.isDesc()) {
            orderedCode.writeBytesDecreasing(v.getBytes(StandardCharsets.UTF_8));
          } else {
            orderedCode.writeBytes(v.getBytes(StandardCharsets.UTF_8));
          }
        } else if (value instanceof ByteArray) {
          ByteArray v = (ByteArray) value;
          if (part.isDesc()) {
            orderedCode.writeBytesDecreasing(v.toByteArray());
          } else {
            orderedCode.writeBytes(v.toByteArray());
          }
        } else if (value instanceof Timestamp) {
          Timestamp v = (Timestamp) value;
          if (part.isDesc()) {
            orderedCode.writeNumDecreasing(v.getSeconds());
            orderedCode.writeNumDecreasing(v.getNanos());
          } else {
            orderedCode.writeNumIncreasing(v.getSeconds());
            orderedCode.writeNumIncreasing(v.getNanos());
          }
        } else if (value instanceof Date) {
          Date v = (Date) value;
          if (part.isDesc()) {
            orderedCode.writeSignedNumDecreasing(encodeDate(v));
          } else {
            orderedCode.writeSignedNumIncreasing(encodeDate(v));
          }
        } else {
          throw new IllegalArgumentException("Unknown key part " + value);
        }
      }
    }
    return orderedCode.getEncodedBytes();
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
}
