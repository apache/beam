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
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.apache.beam.sdk.values.Row;

/**
 *  A {@link Coder} for {@link Row}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class RowCoder extends CustomCoder<Row> {
  private static final Map<FieldType, Coder> CODER_MAP = ImmutableMap.<FieldType, Coder>builder()
      .put(FieldType.BYTE, ByteCoder.of())
      .put(FieldType.INT16, BigEndianShortCoder.of())
      .put(FieldType.INT32, BigEndianIntegerCoder.of())
      .put(FieldType.INT64, BigEndianLongCoder.of())
      .put(FieldType.DECIMAL, BigDecimalCoder.of())
      .put(FieldType.FLOAT, FloatCoder.of())
      .put(FieldType.DOUBLE, DoubleCoder.of())
      .put(FieldType.STRING, StringUtf8Coder.of())
      .put(FieldType.DATETIME, InstantCoder.of())
      .put(FieldType.BOOLEAN, BooleanCoder.of())
      .build();

  private static final Map<FieldType, Integer> ESTIMATED_FIELD_SIZES =
      ImmutableMap.<FieldType, Integer>builder()
          .put(FieldType.BYTE, Byte.BYTES)
          .put(FieldType.INT16, Short.BYTES)
          .put(FieldType.INT32, Integer.BYTES)
          .put(FieldType.INT64, Long.BYTES)
          .put(FieldType.FLOAT, Float.BYTES)
          .put(FieldType.DOUBLE, Double.BYTES)
          .put(FieldType.DECIMAL, 32)
          .put(FieldType.BOOLEAN, 1)
          .put(FieldType.DATETIME, Long.BYTES)
          .build();

  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private Schema schema;

  /**
   * Returns the coder used for a given primitive type.
   */
  public static <T> Coder<T> coderForPrimitiveType(FieldType fieldType) {
    return (Coder<T>) CODER_MAP.get(fieldType);
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
      fieldsSize += estimatedSizeBytes(schema.getField(i).getTypeDescriptor(), row.getValue(i));
    }
    return bitmapSize + fieldsSize;
  }

  private static long estimatedSizeBytes(FieldTypeDescriptor typeDescriptor, Object value) {
    switch (typeDescriptor.getType()) {
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
        return ESTIMATED_FIELD_SIZES.get(typeDescriptor.getType());
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

  Coder getCoder(FieldTypeDescriptor fieldTypeDescriptor) {
    if (FieldType.ARRAY.equals(fieldTypeDescriptor.getType())) {
      return ListCoder.of(getCoder(fieldTypeDescriptor.getComponentType()));
    } else if (FieldType.ROW.equals((fieldTypeDescriptor.getType()))) {
      return RowCoder.of(fieldTypeDescriptor.getRowSchema());
    } else {
      return coderForPrimitiveType(fieldTypeDescriptor.getType());
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
      Coder coder = getCoder(field.getTypeDescriptor());
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
        Coder coder = getCoder(schema.getField(idx).getTypeDescriptor());
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
