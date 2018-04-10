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
package org.apache.beam.sdk.coders;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;

/**
 *  A {@link Coder} for {@link Row}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class RowCoder extends CustomCoder<Row> {
  private static final Map<TypeName, Coder> CODER_MAP = ImmutableMap.<TypeName, Coder>builder()
      .put(TypeName.BYTE, ByteCoder.of())
      .put(TypeName.INT16, BigEndianShortCoder.of())
      .put(TypeName.INT32, BigEndianIntegerCoder.of())
      .put(TypeName.INT64, BigEndianLongCoder.of())
      .put(TypeName.DECIMAL, BigDecimalCoder.of())
      .put(TypeName.FLOAT, FloatCoder.of())
      .put(TypeName.DOUBLE, DoubleCoder.of())
      .put(TypeName.STRING, StringUtf8Coder.of())
      .put(TypeName.DATETIME, InstantCoder.of())
      .put(TypeName.BOOLEAN, BooleanCoder.of())
      .build();

  private static final Map<TypeName, Integer> ESTIMATED_FIELD_SIZES =
      ImmutableMap.<TypeName, Integer>builder()
          .put(TypeName.BYTE, Byte.BYTES)
          .put(TypeName.INT16, Short.BYTES)
          .put(TypeName.INT32, Integer.BYTES)
          .put(TypeName.INT64, Long.BYTES)
          .put(TypeName.FLOAT, Float.BYTES)
          .put(TypeName.DOUBLE, Double.BYTES)
          .put(TypeName.DECIMAL, 32)
          .put(TypeName.BOOLEAN, 1)
          .put(TypeName.DATETIME, Long.BYTES)
          .build();

  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private Schema schema;

  /**
   * Returns the coder used for a given primitive type.
   */
  public static <T> Coder<T> coderForPrimitiveType(TypeName typeName) {
    return (Coder<T>) CODER_MAP.get(typeName);
  }

  /**
   * Return the estimated serialized size of a give row object.
   */
  public static long estimatedSizeBytes(Row row) {
    Schema schema = row.getSchema();
    int fieldCount = schema.getFieldCount();
    int bitmapSize = (((fieldCount - 1) >> 6) + 1) * 8;

    int fieldsSize = 0;
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      fieldsSize += estimatedSizeBytes(schema.getField(i).getType(), row.getValue(i));
    }
    return bitmapSize + fieldsSize;
  }

  private static long estimatedSizeBytes(FieldType typeDescriptor, Object value) {
    switch (typeDescriptor.getTypeName()) {
      case ROW:
        return estimatedSizeBytes((Row) value);
      case ARRAY:
        List list = (List) value;
        long listSizeBytes = 0;
        for (Object elem : list) {
          listSizeBytes += estimatedSizeBytes(typeDescriptor.getComponentType(), elem);
        }
        return 4 + listSizeBytes;
      case STRING:
        // Not always accurate - String.getBytes().length() would be more accurate here, but slower.
        return ((String) value).length();
      default:
        return ESTIMATED_FIELD_SIZES.get(typeDescriptor.getTypeName());
    }
  }

  private RowCoder(Schema schema) {
    this.schema = schema;
  }

  public static RowCoder of(Schema schema) {
    return new RowCoder(schema);
  }

  public Schema getSchema() {
    return schema;
  }

  Coder getCoder(FieldType fieldType) {
    if (TypeName.ARRAY.equals(fieldType.getTypeName())) {
      return ListCoder.of(getCoder(fieldType.getComponentType()));
    } else if (TypeName.ROW.equals((fieldType.getTypeName()))) {
      return RowCoder.of(fieldType.getRowSchema());
    } else {
      return coderForPrimitiveType(fieldType.getTypeName());
    }
  }

  @Override
  public void encode(Row value, OutputStream outStream) throws IOException {
    nullListCoder.encode(scanNullFields(value), outStream);

    for (int idx = 0; idx < value.getFieldCount(); ++idx) {
      Schema.Field field = schema.getField(idx);
      if (value.getValue(idx) == null) {
        continue;
      }
      Coder coder = getCoder(field.getType());
      coder.encode(value.getValue(idx), outStream);
    }
  }

  @Override
  public Row decode(InputStream inStream) throws IOException {
    BitSet nullFields = nullListCoder.decode(inStream);
    List<Object> fieldValues = new ArrayList<>(schema.getFieldCount());
    for (int idx = 0; idx < schema.getFieldCount(); ++idx) {
      if (nullFields.get(idx)) {
        fieldValues.add(null);
      } else {
        Coder coder = getCoder(schema.getField(idx).getType());
        Object value = coder.decode(inStream);
        fieldValues.add(value);
      }
    }
    return Row.withSchema(schema).addValues(fieldValues).build();
  }

  /**
   * Scan {@link Row} to find fields with a NULL value.
   */
  private BitSet scanNullFields(Row row) {
    BitSet nullFields = new BitSet(row.getFieldCount());
    for (int idx = 0; idx < row.getFieldCount(); ++idx) {
      if (row.getValue(idx) == null) {
        nullFields.set(idx);
      }
    }
    return nullFields;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
  }
}
