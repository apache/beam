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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.util.VarInt;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

/** Given the Spanner Schema, efficiently encodes the mutation group. */
class MutationGroupEncoder {
  private static final DateTime MIN_DATE = new DateTime(1, 1, 1, 0, 0);

  private final SpannerSchema schema;
  private final List<String> tables;
  private final Map<String, Integer> tablesIndexes = new HashMap<>();

  public MutationGroupEncoder(SpannerSchema schema) {
    this.schema = schema;
    tables = schema.getTables();

    for (int i = 0; i < tables.size(); i++) {
      tablesIndexes.put(tables.get(i).toLowerCase(), i);
    }
  }

  public byte[] encode(MutationGroup g) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    try {
      VarInt.encode(g.attached().size(), bos);
      for (Mutation m : g) {
        encodeMutation(bos, m);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }

  private static void setBit(byte[] bytes, int i) {
    int word = i / 8;
    int bit = 7 - i % 8;
    bytes[word] = (byte) (bytes[word] | 1 << bit);
  }

  private static boolean getBit(byte[] bytes, int i) {
    int word = i / 8;
    int bit = 7 - i % 8;
    return (bytes[word] & 1 << (bit)) != 0;
  }

  private void encodeMutation(ByteArrayOutputStream bos, Mutation m) throws IOException {
    Mutation.Op op = m.getOperation();
    bos.write(op.ordinal());
    if (op == Mutation.Op.DELETE) {
      encodeDelete(bos, m);
    } else {
      encodeModification(bos, m);
    }
  }

  private void encodeDelete(ByteArrayOutputStream bos, Mutation m) throws IOException {
    String table = m.getTable().toLowerCase();
    int tableIndex = getTableIndex(table);
    VarInt.encode(tableIndex, bos);
    ObjectOutput out = new ObjectOutputStream(bos);
    out.writeObject(m.getKeySet());
  }

  private Integer getTableIndex(String table) {
    Integer result = tablesIndexes.get(table.toLowerCase());
    checkArgument(result != null, "Unknown table '%s'", table);
    return result;
  }

  private Mutation decodeDelete(ByteArrayInputStream bis) throws IOException {
    int tableIndex = VarInt.decodeInt(bis);
    String tableName = tables.get(tableIndex);

    ObjectInputStream in = new ObjectInputStream(bis);
    KeySet keySet;
    try {
      keySet = (KeySet) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return Mutation.delete(tableName, keySet);
  }

  // Encodes a mutation that is not a delete one, using the following format
  // [bitset of modified columns][value of column1][value of column2][value of column3]...
  private void encodeModification(ByteArrayOutputStream bos, Mutation m) throws IOException {
    String tableName = m.getTable().toLowerCase();
    int tableIndex = getTableIndex(tableName);
    VarInt.encode(tableIndex, bos);
    List<SpannerSchema.Column> columns = schema.getColumns(tableName);
    checkArgument(columns != null, "Schema for table " + tableName + " not " + "found");
    Map<String, Value> map = mutationAsMap(m);
    // java.util.BitSet#toByteArray returns array of unpredictable length. Using byte arrays
    // instead.
    int bitsetSize = (columns.size() + 7) / 8;
    byte[] exists = new byte[bitsetSize];
    byte[] nulls = new byte[bitsetSize];
    for (int i = 0; i < columns.size(); i++) {
      String columnName = columns.get(i).getName();
      boolean columnExists = map.containsKey(columnName);
      boolean columnNull = columnExists && map.get(columnName).isNull();
      if (columnExists) {
        setBit(exists, i);
      }
      if (columnNull) {
        setBit(nulls, i);
        map.remove(columnName);
      }
    }
    bos.write(exists);
    bos.write(nulls);
    for (int i = 0; i < columns.size(); i++) {
      if (!getBit(exists, i) || getBit(nulls, i)) {
        continue;
      }
      SpannerSchema.Column column = columns.get(i);
      Value value = map.remove(column.getName());
      encodeValue(bos, value);
    }
    checkArgument(
        map.isEmpty(), "Columns %s were not defined in table %s", map.keySet(), m.getTable());
  }

  private void encodeValue(ByteArrayOutputStream bos, Value value) throws IOException {
    switch (value.getType().getCode()) {
      case ARRAY:
        encodeArray(bos, value);
        break;
      default:
        encodePrimitive(bos, value);
    }
  }

  private void encodeArray(ByteArrayOutputStream bos, Value value) throws IOException {
    // TODO: avoid using Java serialization here.
    ObjectOutputStream out = new ObjectOutputStream(bos);
    switch (value.getType().getArrayElementType().getCode()) {
      case BOOL:
        {
          out.writeObject(new ArrayList<>(value.getBoolArray()));
          break;
        }
      case INT64:
        {
          out.writeObject(new ArrayList<>(value.getInt64Array()));
          break;
        }
      case FLOAT64:
        {
          out.writeObject(new ArrayList<>(value.getFloat64Array()));
          break;
        }
      case STRING:
        {
          out.writeObject(new ArrayList<>(value.getStringArray()));
          break;
        }
      case BYTES:
        {
          out.writeObject(new ArrayList<>(value.getBytesArray()));
          break;
        }
      case TIMESTAMP:
        {
          out.writeObject(new ArrayList<>(value.getTimestampArray()));
          break;
        }
      case DATE:
        {
          out.writeObject(new ArrayList<>(value.getDateArray()));
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown type " + value.getType());
    }
  }

  private void encodePrimitive(ByteArrayOutputStream bos, Value value) throws IOException {
    switch (value.getType().getCode()) {
      case BOOL:
        bos.write(value.getBool() ? 1 : 0);
        break;
      case INT64:
        VarInt.encode(value.getInt64(), bos);
        break;
      case FLOAT64:
        new DataOutputStream(bos).writeDouble(value.getFloat64());
        break;
      case STRING:
        {
          String str = value.getString();
          byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
          VarInt.encode(bytes.length, bos);
          bos.write(bytes);
          break;
        }
      case BYTES:
        {
          ByteArray bytes = value.getBytes();
          VarInt.encode(bytes.length(), bos);
          bos.write(bytes.toByteArray());
          break;
        }
      case TIMESTAMP:
        {
          Timestamp timestamp = value.getTimestamp();
          VarInt.encode(timestamp.getSeconds(), bos);
          VarInt.encode(timestamp.getNanos(), bos);
          break;
        }
      case DATE:
        {
          Date date = value.getDate();
          VarInt.encode(encodeDate(date), bos);
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown type " + value.getType());
    }
  }

  public MutationGroup decode(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

    try {
      int numMutations = VarInt.decodeInt(bis);
      Mutation primary = decodeMutation(bis);
      List<Mutation> attached = new ArrayList<>(numMutations);
      for (int i = 0; i < numMutations; i++) {
        attached.add(decodeMutation(bis));
      }
      return MutationGroup.create(primary, attached);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Mutation decodeMutation(ByteArrayInputStream bis) throws IOException {
    Mutation.Op op = Mutation.Op.values()[bis.read()];
    if (op == Mutation.Op.DELETE) {
      return decodeDelete(bis);
    }
    return decodeModification(bis, op);
  }

  private Mutation decodeModification(ByteArrayInputStream bis, Mutation.Op op) throws IOException {
    int tableIndex = VarInt.decodeInt(bis);
    String tableName = tables.get(tableIndex);

    Mutation.WriteBuilder m;
    switch (op) {
      case INSERT:
        m = Mutation.newInsertBuilder(tableName);
        break;
      case INSERT_OR_UPDATE:
        m = Mutation.newInsertOrUpdateBuilder(tableName);
        break;
      case REPLACE:
        m = Mutation.newReplaceBuilder(tableName);
        break;
      case UPDATE:
        m = Mutation.newUpdateBuilder(tableName);
        break;
      default:
        throw new IllegalArgumentException("Unknown operation " + op);
    }
    List<SpannerSchema.Column> columns = schema.getColumns(tableName);
    int bitsetSize = (columns.size() + 7) / 8;
    byte[] exists = readBytes(bis, bitsetSize);
    byte[] nulls = readBytes(bis, bitsetSize);

    for (int i = 0; i < columns.size(); i++) {
      if (!getBit(exists, i)) {
        continue;
      }
      SpannerSchema.Column column = columns.get(i);
      boolean isNull = getBit(nulls, i);
      Type type = column.getType();
      String fieldName = column.getName();
      switch (type.getCode()) {
        case ARRAY:
          try {
            decodeArray(bis, fieldName, type, isNull, m);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          decodePrimitive(bis, fieldName, type, isNull, m);
      }
    }
    return m.build();
  }

  private void decodeArray(
      ByteArrayInputStream bis,
      String fieldName,
      Type type,
      boolean isNull,
      Mutation.WriteBuilder m)
      throws IOException, ClassNotFoundException {
    // TODO: avoid using Java serialization here.
    switch (type.getArrayElementType().getCode()) {
      case BOOL:
        {
          if (isNull) {
            m.set(fieldName).toBoolArray((Iterable<Boolean>) null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toBoolArray((List<Boolean>) out.readObject());
          }
          break;
        }
      case INT64:
        {
          if (isNull) {
            m.set(fieldName).toInt64Array((Iterable<Long>) null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toInt64Array((List<Long>) out.readObject());
          }
          break;
        }
      case FLOAT64:
        {
          if (isNull) {
            m.set(fieldName).toFloat64Array((Iterable<Double>) null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toFloat64Array((List<Double>) out.readObject());
          }
          break;
        }
      case STRING:
        {
          if (isNull) {
            m.set(fieldName).toStringArray(null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toStringArray((List<String>) out.readObject());
          }
          break;
        }
      case BYTES:
        {
          if (isNull) {
            m.set(fieldName).toBytesArray(null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toBytesArray((List<ByteArray>) out.readObject());
          }
          break;
        }
      case TIMESTAMP:
        {
          if (isNull) {
            m.set(fieldName).toTimestampArray(null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toTimestampArray((List<Timestamp>) out.readObject());
          }
          break;
        }
      case DATE:
        {
          if (isNull) {
            m.set(fieldName).toDateArray(null);
          } else {
            ObjectInputStream out = new ObjectInputStream(bis);
            m.set(fieldName).toDateArray((List<Date>) out.readObject());
          }
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  private void decodePrimitive(
      ByteArrayInputStream bis,
      String fieldName,
      Type type,
      boolean isNull,
      Mutation.WriteBuilder m)
      throws IOException {
    switch (type.getCode()) {
      case BOOL:
        if (isNull) {
          m.set(fieldName).to((Boolean) null);
        } else {
          m.set(fieldName).to(bis.read() != 0);
        }
        break;
      case INT64:
        if (isNull) {
          m.set(fieldName).to((Long) null);
        } else {
          m.set(fieldName).to(VarInt.decodeLong(bis));
        }
        break;
      case FLOAT64:
        if (isNull) {
          m.set(fieldName).to((Double) null);
        } else {
          m.set(fieldName).to(new DataInputStream(bis).readDouble());
        }
        break;
      case STRING:
        {
          if (isNull) {
            m.set(fieldName).to((String) null);
          } else {
            int len = VarInt.decodeInt(bis);
            byte[] bytes = readBytes(bis, len);
            m.set(fieldName).to(new String(bytes, StandardCharsets.UTF_8));
          }
          break;
        }
      case BYTES:
        {
          if (isNull) {
            m.set(fieldName).to((ByteArray) null);
          } else {
            int len = VarInt.decodeInt(bis);
            byte[] bytes = readBytes(bis, len);
            m.set(fieldName).to(ByteArray.copyFrom(bytes));
          }
          break;
        }
      case TIMESTAMP:
        {
          if (isNull) {
            m.set(fieldName).to((Timestamp) null);
          } else {
            long seconds = VarInt.decodeLong(bis);
            int nanoseconds = VarInt.decodeInt(bis);
            m.set(fieldName).to(Timestamp.ofTimeSecondsAndNanos(seconds, nanoseconds));
          }
          break;
        }
      case DATE:
        {
          if (isNull) {
            m.set(fieldName).to((Date) null);
          } else {
            int days = VarInt.decodeInt(bis);
            m.set(fieldName).to(decodeDate(days));
          }
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  private byte[] readBytes(ByteArrayInputStream bis, int len) throws IOException {
    byte[] tmp = new byte[len];
    new DataInputStream(bis).readFully(tmp);
    return tmp;
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

  private static Date decodeDate(int daysSinceEpoch) {

    DateTime jodaDate = MIN_DATE.plusDays(daysSinceEpoch);

    return Date.fromYearMonthDay(
        jodaDate.getYear(), jodaDate.getMonthOfYear(), jodaDate.getDayOfMonth());
  }
}
