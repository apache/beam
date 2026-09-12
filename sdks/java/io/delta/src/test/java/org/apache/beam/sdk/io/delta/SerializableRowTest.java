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
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SerializableRowTest {

  @Test
  public void testAllTypesReadWriteSerialization() throws Exception {
    StructType structSchema =
        new StructType(Arrays.asList(new StructField("nested_int", IntegerType.INTEGER, false)));

    StructType schema =
        new StructType(
            Arrays.asList(
                new StructField("boolean", BooleanType.BOOLEAN, true),
                new StructField("byte", ByteType.BYTE, true),
                new StructField("short", ShortType.SHORT, true),
                new StructField("integer", IntegerType.INTEGER, true),
                new StructField("long", LongType.LONG, true),
                new StructField("float", FloatType.FLOAT, true),
                new StructField("double", DoubleType.DOUBLE, true),
                new StructField("string", StringType.STRING, true),
                new StructField("binary", BinaryType.BINARY, true),
                new StructField("decimal", new DecimalType(10, 2), true),
                new StructField("date", DateType.DATE, true),
                new StructField("timestamp", TimestampType.TIMESTAMP, true),
                new StructField("struct", structSchema, true),
                new StructField("array", new ArrayType(StringType.STRING, true), true),
                new StructField(
                    "map", new MapType(StringType.STRING, IntegerType.INTEGER, true), true)));

    Map<String, Object> values = new LinkedHashMap<>();
    values.put("boolean", true);
    values.put("byte", (byte) 1);
    values.put("short", (short) 2);
    values.put("integer", 3);
    values.put("long", 4L);
    values.put("float", 5.0f);
    values.put("double", 6.0d);
    values.put("string", "hello");
    values.put("binary", new byte[] {7, 8});
    values.put("decimal", new BigDecimal("9.20"));
    values.put("date", 16543);
    values.put("timestamp", 16543000000L);

    Map<String, Object> nestedValues = new LinkedHashMap<>();
    nestedValues.put("nested_int", 42);
    values.put("struct", new FakeRow(structSchema, nestedValues));

    values.put("array", Arrays.asList("a", "b", null));

    Map<String, Object> mapValues = new LinkedHashMap<>();
    mapValues.put("key1", 100);
    mapValues.put("key2", null);
    values.put("map", mapValues);

    FakeRow originalRow = new FakeRow(schema, values);
    SerializableRow serializableRow = new SerializableRow(originalRow);

    verifyRowContents(serializableRow, schema, values);

    // Test equals and hashCode
    SerializableRow serializableRow2 = new SerializableRow(new FakeRow(schema, values));
    Assert.assertEquals(serializableRow, serializableRow2);
    Assert.assertEquals(serializableRow.hashCode(), serializableRow2.hashCode());
    Assert.assertNotNull(serializableRow.toString());

    // Serialize and Deserialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(serializableRow);
    }

    byte[] bytes = baos.toByteArray();
    SerializableRow deserializedRow;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      deserializedRow = (SerializableRow) ois.readObject();
    }

    verifyRowContents(deserializedRow, schema, values);
    Assert.assertEquals(serializableRow, deserializedRow);
  }

  @Test
  public void testNullValuesReadWriteSerialization() throws Exception {
    StructType structSchema =
        new StructType(Arrays.asList(new StructField("nested_int", IntegerType.INTEGER, true)));

    StructType schema =
        new StructType(
            Arrays.asList(
                new StructField("boolean", BooleanType.BOOLEAN, true),
                new StructField("byte", ByteType.BYTE, true),
                new StructField("short", ShortType.SHORT, true),
                new StructField("integer", IntegerType.INTEGER, true),
                new StructField("long", LongType.LONG, true),
                new StructField("float", FloatType.FLOAT, true),
                new StructField("double", DoubleType.DOUBLE, true),
                new StructField("string", StringType.STRING, true),
                new StructField("binary", BinaryType.BINARY, true),
                new StructField("decimal", new DecimalType(10, 2), true),
                new StructField("date", DateType.DATE, true),
                new StructField("timestamp", TimestampType.TIMESTAMP, true),
                new StructField("struct", structSchema, true),
                new StructField("array", new ArrayType(StringType.STRING, true), true),
                new StructField(
                    "map", new MapType(StringType.STRING, IntegerType.INTEGER, true), true)));

    Map<String, Object> nullValues = new LinkedHashMap<>();
    for (StructField field : schema.fields()) {
      nullValues.put(field.getName(), null);
    }

    FakeRow originalRow = new FakeRow(schema, nullValues);
    SerializableRow serializableRow = new SerializableRow(originalRow);

    verifyRowContents(serializableRow, schema, nullValues);

    // Serialize and Deserialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(serializableRow);
    }

    byte[] bytes = baos.toByteArray();
    SerializableRow deserializedRow;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      deserializedRow = (SerializableRow) ois.readObject();
    }

    verifyRowContents(deserializedRow, schema, nullValues);
    Assert.assertEquals(serializableRow, deserializedRow);
  }

  private void verifyRowContents(Row row, StructType schema, Map<String, Object> expectedValues) {
    Assert.assertEquals(schema.toString(), row.getSchema().toString());
    int i = 0;
    for (StructField field : schema.fields()) {
      Object expected = expectedValues.get(field.getName());
      Assert.assertEquals(expected == null, row.isNullAt(i));
      if (expected != null) {
        DataType type = field.getDataType();
        if (type instanceof BooleanType) {
          Assert.assertEquals(expected, row.getBoolean(i));
        } else if (type instanceof ByteType) {
          Assert.assertEquals(expected, row.getByte(i));
        } else if (type instanceof ShortType) {
          Assert.assertEquals(expected, row.getShort(i));
        } else if (type instanceof IntegerType) {
          Assert.assertEquals(expected, row.getInt(i));
        } else if (type instanceof LongType) {
          Assert.assertEquals(expected, row.getLong(i));
        } else if (type instanceof FloatType) {
          Assert.assertEquals(expected, row.getFloat(i));
        } else if (type instanceof DoubleType) {
          Assert.assertEquals(expected, row.getDouble(i));
        } else if (type instanceof StringType) {
          Assert.assertEquals(expected, row.getString(i));
        } else if (type instanceof BinaryType) {
          Assert.assertArrayEquals((byte[]) expected, row.getBinary(i));
        } else if (type instanceof DecimalType) {
          Assert.assertEquals(expected, row.getDecimal(i));
        } else if (type instanceof DateType) {
          Assert.assertEquals(expected, row.getInt(i));
        } else if (type instanceof TimestampType) {
          Assert.assertEquals(expected, row.getLong(i));
        } else if (type instanceof StructType) {
          Row actualStruct = row.getStruct(i);
          Assert.assertNotNull(actualStruct);
          Row expectedStruct = (Row) expected;
          Assert.assertEquals(
              expectedStruct.getSchema().toString(), actualStruct.getSchema().toString());
          for (int j = 0; j < expectedStruct.getSchema().fields().size(); j++) {
            Assert.assertEquals(expectedStruct.isNullAt(j), actualStruct.isNullAt(j));
            if (!expectedStruct.isNullAt(j)) {
              Assert.assertEquals(expectedStruct.getInt(j), actualStruct.getInt(j));
            }
          }
        } else if (type instanceof ArrayType) {
          ArrayValue actualArray = row.getArray(i);
          Assert.assertNotNull(actualArray);
          List<?> expectedList = (List<?>) expected;
          Assert.assertEquals(expectedList.size(), actualArray.getSize());
          ColumnVector vector = actualArray.getElements();
          for (int j = 0; j < expectedList.size(); j++) {
            Assert.assertEquals(expectedList.get(j) == null, vector.isNullAt(j));
            if (expectedList.get(j) != null) {
              Assert.assertEquals(expectedList.get(j), vector.getString(j));
            }
          }
        } else if (type instanceof MapType) {
          MapValue actualMap = row.getMap(i);
          Assert.assertNotNull(actualMap);
          Map<?, ?> expectedMap = (Map<?, ?>) expected;
          Assert.assertEquals(expectedMap.size(), actualMap.getSize());
          ColumnVector keys = actualMap.getKeys();
          ColumnVector valuesVector = actualMap.getValues();
          int j = 0;
          for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
            Assert.assertEquals(entry.getKey(), keys.getString(j));
            Assert.assertEquals(entry.getValue() == null, valuesVector.isNullAt(j));
            if (entry.getValue() != null) {
              Assert.assertEquals(entry.getValue(), valuesVector.getInt(j));
            }
            j++;
          }
        }
      }
      i++;
    }
  }

  private static class FakeRow implements Row {
    private final StructType schema;
    private final Map<String, Object> values;

    FakeRow(StructType schema, Map<String, Object> values) {
      this.schema = schema;
      this.values = values;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public boolean isNullAt(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return values.get(fieldName) == null;
    }

    @Override
    public boolean getBoolean(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Boolean) values.get(fieldName);
    }

    @Override
    public byte getByte(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Byte) values.get(fieldName);
    }

    @Override
    public short getShort(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Short) values.get(fieldName);
    }

    @Override
    public int getInt(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Integer) values.get(fieldName);
    }

    @Override
    public long getLong(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Long) values.get(fieldName);
    }

    @Override
    public float getFloat(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Float) values.get(fieldName);
    }

    @Override
    public double getDouble(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Double) values.get(fieldName);
    }

    @Override
    public String getString(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (String) values.get(fieldName);
    }

    @Override
    public byte[] getBinary(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (byte[]) values.get(fieldName);
    }

    @Override
    public BigDecimal getDecimal(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (BigDecimal) values.get(fieldName);
    }

    @Override
    public Row getStruct(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      return (Row) values.get(fieldName);
    }

    @Override
    public ArrayValue getArray(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      List<?> list = (List<?>) values.get(fieldName);
      if (list == null) {
        return null;
      }
      DataType elementType = ((ArrayType) schema.fields().get(ord).getDataType()).getElementType();
      return new FakeArrayValue(list, elementType);
    }

    @Override
    public MapValue getMap(int ord) {
      String fieldName = schema.fields().get(ord).getName();
      Map<?, ?> map = (Map<?, ?>) values.get(fieldName);
      if (map == null) {
        return null;
      }
      MapType mapType = (MapType) schema.fields().get(ord).getDataType();
      return new FakeMapValue(map, mapType.getKeyType(), mapType.getValueType());
    }
  }

  private static class FakeArrayValue implements ArrayValue {
    private final List<?> list;
    private final DataType elementType;

    FakeArrayValue(List<?> list, DataType elementType) {
      this.list = list;
      this.elementType = elementType;
    }

    @Override
    public int getSize() {
      return list.size();
    }

    @Override
    public ColumnVector getElements() {
      return new FakeColumnVector(elementType, list);
    }
  }

  private static class FakeMapValue implements MapValue {
    private final Map<?, ?> map;
    private final DataType keyType;
    private final DataType valueType;

    FakeMapValue(Map<?, ?> map, DataType keyType, DataType valueType) {
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
      return new FakeColumnVector(keyType, new ArrayList<>(map.keySet()));
    }

    @Override
    public ColumnVector getValues() {
      return new FakeColumnVector(valueType, new ArrayList<>(map.values()));
    }
  }

  private static class FakeColumnVector implements ColumnVector {
    private final DataType dataType;
    private final List<Object> list;

    FakeColumnVector(DataType dataType, List<?> list) {
      this.dataType = dataType;
      this.list = new ArrayList<>(list);
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
      return (Boolean) list.get(rowId);
    }

    @Override
    public byte getByte(int rowId) {
      return (Byte) list.get(rowId);
    }

    @Override
    public short getShort(int rowId) {
      return (Short) list.get(rowId);
    }

    @Override
    public int getInt(int rowId) {
      return (Integer) list.get(rowId);
    }

    @Override
    public long getLong(int rowId) {
      return (Long) list.get(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return (Float) list.get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return (Double) list.get(rowId);
    }

    @Override
    public String getString(int rowId) {
      return (String) list.get(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
      return (byte[]) list.get(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      return (BigDecimal) list.get(rowId);
    }

    @Override
    public void close() {}
  }
}
