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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A serializable wrapper for Delta {@link Row} that implements the {@link Row} interface itself,
 * allowing worker nodes to access serialized Row objects using standard Delta Kernel APIs.
 */
public class SerializableRow implements Row, Serializable {
  private static final long serialVersionUID = 1L;

  private final SerializableStructType schema;
  private final @Nullable Object[] values;

  public SerializableRow(Row row) {
    this.schema = new SerializableStructType(row.getSchema());
    StructType structType = row.getSchema();
    int numFields = structType.fields().size();
    this.values = new Object[numFields];
    for (int i = 0; i < numFields; i++) {
      DataType type = structType.fields().get(i).getDataType();
      this.values[i] = getValue(row, i, type);
    }
  }

  @Override
  public StructType getSchema() {
    return schema.get();
  }

  @Override
  public boolean isNullAt(int ord) {
    return values == null || values[ord] == null;
  }

  @Override
  public boolean getBoolean(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Boolean) val : false;
  }

  @Override
  public byte getByte(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Byte) val : 0;
  }

  @Override
  public short getShort(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Short) val : 0;
  }

  @Override
  public int getInt(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Integer) val : 0;
  }

  @Override
  public long getLong(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Long) val : 0L;
  }

  @Override
  public float getFloat(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Float) val : 0.0f;
  }

  @Override
  public double getDouble(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    return val != null ? (Double) val : 0.0d;
  }

  @Override
  @SuppressWarnings("nullness")
  public String getString(int ord) {
    return (String) Objects.requireNonNull(values)[ord];
  }

  @Override
  @SuppressWarnings("nullness")
  public byte[] getBinary(int ord) {
    return (byte[]) Objects.requireNonNull(values)[ord];
  }

  @Override
  @SuppressWarnings("nullness")
  public BigDecimal getDecimal(int ord) {
    return (BigDecimal) Objects.requireNonNull(values)[ord];
  }

  @Override
  @SuppressWarnings("nullness")
  public Row getStruct(int ord) {
    return (Row) Objects.requireNonNull(values)[ord];
  }

  @Override
  @SuppressWarnings({"unchecked", "nullness"})
  public ArrayValue getArray(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    if (val == null) {
      return null;
    }
    DataType elementType =
        ((ArrayType) getSchema().fields().get(ord).getDataType()).getElementType();
    return new SerializableArrayValue((List<@Nullable Object>) val, elementType);
  }

  @Override
  @SuppressWarnings({"unchecked", "nullness"})
  public MapValue getMap(int ord) {
    Object val = Objects.requireNonNull(values)[ord];
    if (val == null) {
      return null;
    }
    MapType mapType = (MapType) getSchema().fields().get(ord).getDataType();
    return new SerializableMapValue(
        (Map<Object, @Nullable Object>) val, mapType.getKeyType(), mapType.getValueType());
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SerializableRow)) {
      return false;
    }
    SerializableRow that = (SerializableRow) o;
    return Objects.equals(schema, that.schema) && java.util.Arrays.deepEquals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, java.util.Arrays.deepHashCode(values));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SerializableRow{schema=").append(schema).append(", values=[");
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(values[i]);
      }
    }
    sb.append("]}");
    return sb.toString();
  }

  private static @Nullable Object getValue(Row row, int index, DataType type) {
    if (row.isNullAt(index)) {
      return null;
    }
    if (type instanceof BooleanType) {
      return row.getBoolean(index);
    } else if (type instanceof ByteType) {
      return row.getByte(index);
    } else if (type instanceof ShortType) {
      return row.getShort(index);
    } else if (type instanceof IntegerType) {
      return row.getInt(index);
    } else if (type instanceof LongType) {
      return row.getLong(index);
    } else if (type instanceof FloatType) {
      return row.getFloat(index);
    } else if (type instanceof DoubleType) {
      return row.getDouble(index);
    } else if (type instanceof StringType) {
      return row.getString(index);
    } else if (type instanceof BinaryType) {
      return row.getBinary(index);
    } else if (type instanceof DecimalType) {
      return row.getDecimal(index);
    } else if (type instanceof StructType) {
      return new SerializableRow(row.getStruct(index));
    } else if (type instanceof DateType) {
      return row.getInt(index);
    } else if (type instanceof TimestampType) {
      return row.getLong(index);
    } else if (type instanceof ArrayType) {
      ArrayValue arr = row.getArray(index);
      return convertArray(arr, (ArrayType) type);
    } else if (type instanceof MapType) {
      MapValue map = row.getMap(index);
      return convertMap(map, (MapType) type);
    }
    throw new IllegalArgumentException("Unsupported type: " + type);
  }

  private static @Nullable Object getVectorValue(ColumnVector vector, int index, DataType type) {
    if (vector.isNullAt(index)) {
      return null;
    }
    if (type instanceof BooleanType) {
      return vector.getBoolean(index);
    } else if (type instanceof ByteType) {
      return vector.getByte(index);
    } else if (type instanceof ShortType) {
      return vector.getShort(index);
    } else if (type instanceof IntegerType) {
      return vector.getInt(index);
    } else if (type instanceof LongType) {
      return vector.getLong(index);
    } else if (type instanceof FloatType) {
      return vector.getFloat(index);
    } else if (type instanceof DoubleType) {
      return vector.getDouble(index);
    } else if (type instanceof StringType) {
      return vector.getString(index);
    } else if (type instanceof BinaryType) {
      return vector.getBinary(index);
    } else if (type instanceof DecimalType) {
      return vector.getDecimal(index);
    } else if (type instanceof StructType) {
      StructType structType = (StructType) type;
      int numFields = structType.fields().size();
      ColumnVector[] childFields = new ColumnVector[numFields];
      for (int j = 0; j < numFields; j++) {
        childFields[j] = vector.getChild(j);
      }
      return new SerializableRow(new VectorRow(structType, childFields, index));
    } else if (type instanceof DateType) {
      return vector.getInt(index);
    } else if (type instanceof TimestampType) {
      return vector.getLong(index);
    } else if (type instanceof ArrayType) {
      ArrayValue arr = vector.getArray(index);
      return convertArray(arr, (ArrayType) type);
    } else if (type instanceof MapType) {
      MapValue map = vector.getMap(index);
      return convertMap(map, (MapType) type);
    }
    throw new IllegalArgumentException("Unsupported vector type: " + type);
  }

  private static List<@Nullable Object> convertArray(ArrayValue arr, ArrayType arrayType) {
    int size = arr.getSize();
    ColumnVector elements = arr.getElements();
    DataType elementType = arrayType.getElementType();
    List<@Nullable Object> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(getVectorValue(elements, i, elementType));
    }
    return result;
  }

  private static Map<Object, @Nullable Object> convertMap(MapValue map, MapType mapType) {
    int size = map.getSize();
    ColumnVector keys = map.getKeys();
    ColumnVector values = map.getValues();
    DataType keyType = mapType.getKeyType();
    DataType valueType = mapType.getValueType();
    Map<Object, @Nullable Object> result = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      Object key = getVectorValue(keys, i, keyType);
      if (key != null) {
        result.put(key, getVectorValue(values, i, valueType));
      }
    }
    return result;
  }

  private static class VectorRow implements Row {
    private final StructType schema;
    private final ColumnVector[] fields;
    private final int rowIndex;

    VectorRow(StructType schema, ColumnVector[] fields, int rowIndex) {
      this.schema = schema;
      this.fields = fields;
      this.rowIndex = rowIndex;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public boolean isNullAt(int ord) {
      return fields[ord].isNullAt(rowIndex);
    }

    @Override
    public boolean getBoolean(int ord) {
      return fields[ord].getBoolean(rowIndex);
    }

    @Override
    public byte getByte(int ord) {
      return fields[ord].getByte(rowIndex);
    }

    @Override
    public short getShort(int ord) {
      return fields[ord].getShort(rowIndex);
    }

    @Override
    public int getInt(int ord) {
      return fields[ord].getInt(rowIndex);
    }

    @Override
    public long getLong(int ord) {
      return fields[ord].getLong(rowIndex);
    }

    @Override
    public float getFloat(int ord) {
      return fields[ord].getFloat(rowIndex);
    }

    @Override
    public double getDouble(int ord) {
      return fields[ord].getDouble(rowIndex);
    }

    @Override
    public String getString(int ord) {
      return fields[ord].getString(rowIndex);
    }

    @Override
    public byte[] getBinary(int ord) {
      return fields[ord].getBinary(rowIndex);
    }

    @Override
    public BigDecimal getDecimal(int ord) {
      return fields[ord].getDecimal(rowIndex);
    }

    @Override
    public Row getStruct(int ord) {
      StructType childSchema = (StructType) schema.fields().get(ord).getDataType();
      int numFields = childSchema.fields().size();
      ColumnVector[] childFields = new ColumnVector[numFields];
      for (int j = 0; j < numFields; j++) {
        childFields[j] = fields[ord].getChild(j);
      }
      return new VectorRow(childSchema, childFields, rowIndex);
    }

    @Override
    public ArrayValue getArray(int ord) {
      return fields[ord].getArray(rowIndex);
    }

    @Override
    public MapValue getMap(int ord) {
      return fields[ord].getMap(rowIndex);
    }
  }

  private static class ListColumnVector implements ColumnVector {
    private final DataType dataType;
    private final List<@Nullable Object> list;

    @SuppressWarnings("unchecked")
    ListColumnVector(DataType dataType, List<?> list) {
      this.dataType = dataType;
      this.list = (List<@Nullable Object>) list;
    }

    @Override
    public DataType getDataType() {
      return dataType;
    }

    @Override
    public int getSize() {
      return list.size();
    }

    @Override
    public boolean isNullAt(int rowId) {
      return list.get(rowId) == null;
    }

    @Override
    public boolean getBoolean(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Boolean) val : false;
    }

    @Override
    public byte getByte(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Byte) val : 0;
    }

    @Override
    public short getShort(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Short) val : 0;
    }

    @Override
    public int getInt(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Integer) val : 0;
    }

    @Override
    public long getLong(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Long) val : 0L;
    }

    @Override
    public float getFloat(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Float) val : 0.0f;
    }

    @Override
    public double getDouble(int rowId) {
      Object val = list.get(rowId);
      return val != null ? (Double) val : 0.0d;
    }

    @Override
    @SuppressWarnings("nullness")
    public String getString(int rowId) {
      return (String) list.get(rowId);
    }

    @Override
    @SuppressWarnings("nullness")
    public byte[] getBinary(int rowId) {
      return (byte[]) list.get(rowId);
    }

    @Override
    @SuppressWarnings("nullness")
    public BigDecimal getDecimal(int rowId) {
      return (BigDecimal) list.get(rowId);
    }

    @Override
    public void close() {}
  }

  private static class SerializableArrayValue implements ArrayValue {
    private final List<?> list;
    private final DataType elementType;

    SerializableArrayValue(List<?> list, DataType elementType) {
      this.list = list;
      this.elementType = elementType;
    }

    @Override
    public int getSize() {
      return list.size();
    }

    @Override
    public ColumnVector getElements() {
      return new ListColumnVector(elementType, list);
    }
  }

  private static class SerializableMapValue implements MapValue {
    private final Map<?, ?> map;
    private final DataType keyType;
    private final DataType valueType;

    SerializableMapValue(Map<?, ?> map, DataType keyType, DataType valueType) {
      this.map = map;
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public int getSize() {
      return map.size();
    }

    @Override
    public ColumnVector getKeys() {
      return new ListColumnVector(keyType, new ArrayList<>(map.keySet()));
    }

    @Override
    public ColumnVector getValues() {
      return new ListColumnVector(valueType, new ArrayList<>(map.values()));
    }
  }
}
