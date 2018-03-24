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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.GregorianCalendar;
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
  /**
   * {@link Coder} for Java type {@link Boolean}.
   */
  public static class BooleanCoder extends AtomicCoder<Boolean> {
    private static final BooleanCoder INSTANCE = new BooleanCoder();

    public static BooleanCoder of() {
      return INSTANCE;
    }

    private BooleanCoder() {
    }

    @Override
    public void encode(Boolean value, OutputStream outStream) throws IOException {
      new DataOutputStream(outStream).writeBoolean(value);
    }

    @Override
    public Boolean decode(InputStream inStream) throws IOException {
      return new DataInputStream(inStream).readBoolean();
    }
  }

  private static final Map<FieldType, Coder> CODER_MAP = ImmutableMap.<FieldType, Coder>builder()
      .put(FieldType.BYTE, ByteCoder.of())
      .put(FieldType.INT16, BigEndianShortCoder.of())
      .put(FieldType.INT32, BigEndianIntegerCoder.of())
      .put(FieldType.INT64, BigEndianLongCoder.of())
      .put(FieldType.FLOAT, DoubleCoder.of())
      .put(FieldType.DOUBLE, DoubleCoder.of())
      .put(FieldType.STRING, StringUtf8Coder.of())
      .put(FieldType.DATETIME, InstantCoder.of())
      .put(FieldType.BOOLEAN, BooleanCoder.of())
      .build();
  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private Schema schema;

  private RowCoder(Schema schema) {
    this.schema = schema;
  }

  public static RowCoder of(Schema schema) {
    return new RowCoder(schema);
  }

  public Schema getSchema() {
    return schema;
  }

  public static <T> Coder<T> coderForPrimitiveType(FieldType fieldType) {
    return (Coder<T>) CODER_MAP.get(fieldType);
  }

  Coder getCoder(FieldTypeDescriptor fieldTypeDescriptor) {
    if (FieldType.ARRAY.equals(fieldTypeDescriptor.getType())) {
      return ListCoder.of(getCoder(fieldTypeDescriptor.getComponentType()));
    } else if (FieldType.ROW.equals((fieldTypeDescriptor.getType()))) {
      return RowCoder.of(fieldTypeDescriptor.getRowSchema());
    } else {
      return CODER_MAP.get(fieldTypeDescriptor.getType());
    }
  }

  @Override
  public void encode(Row value, OutputStream outStream) throws IOException {
    nullListCoder.encode(scanNullFields(value), outStream);
    for (int idx = 0; idx < value.getFieldCount(); ++idx) {
      if (value.getValue(idx) == null) {
        continue;
      }
      getCoder(schema.getField(idx).getTypeDescriptor()).encode(value.getValue(idx), outStream);
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
        fieldValues.add(getCoder(schema.getField(idx).getTypeDescriptor()).decode(inStream));
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
