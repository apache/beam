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

package org.apache.beam.sdk.nexmark.model.sql;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * {@link KnownSize} implementation to estimate the size of a {@link Row},
 * similar to Java model. NexmarkLauncher/Queries infrastructure expects the events to
 * be able to quickly provide the estimates of their sizes.
 *
 * <p>The {@link Row} size is calculated at creation time.
 *
 * <p>Field sizes are sizes of Java types described in {@link RowSqlType}. Except strings,
 * which are assumed to be taking 1-byte per character plus 1 byte size.
 *
 * <p>TODO(reuvenlax): This needs to be coupled to Row, not a part of SQL.
 */
public class RowSize implements KnownSize {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  public static final Coder<RowSize> CODER = new CustomCoder<RowSize>() {
    @Override
    public void encode(RowSize rowSize, OutputStream outStream) throws IOException {

      LONG_CODER.encode(rowSize.sizeInBytes(), outStream);
    }

    @Override
    public RowSize decode(InputStream inStream) throws IOException {
      return new RowSize(LONG_CODER.decode(inStream));
    }
  };

  private static final Map<FieldType, Integer> ESTIMATED_FIELD_SIZES =
      ImmutableMap.<FieldType, Integer>builder()
          .put(FieldType.BYTE, bytes(Byte.SIZE))
          .put(FieldType.INT16, bytes(Short.SIZE))
          .put(FieldType.INT64, bytes(Integer.SIZE))
          .put(FieldType.INT64, bytes(Long.SIZE))
          .put(FieldType.FLOAT, bytes(Float.SIZE))
          .put(FieldType.DOUBLE, bytes(Double.SIZE))
          .put(FieldType.DECIMAL, 32)
          .put(FieldType.BOOLEAN, 1)
          .put(FieldType.TIME, bytes(Long.SIZE))
          .put(FieldType.DATE, bytes(Long.SIZE))
          .put(FieldType.DATETIME, bytes(Long.SIZE))
          .build();

  public static ParDo.SingleOutput<Row, RowSize> parDo() {
    return ParDo.of(new DoFn<Row, RowSize>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(RowSize.of(c.element()));
      }
    });
  }

  public static RowSize of(Row row) {
    return new RowSize(sizeInBytes(row));
  }

  private static long sizeInBytes(Row row) {
    Schema schema = row.getSchema();
    long size = 1; // nulls bitset

    for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
      FieldTypeDescriptor fieldTypeDescriptor = schema.getField(fieldIndex).getTypeDescriptor();
      if (FieldType.ARRAY.equals(fieldTypeDescriptor.getType())) {
        // TODO(reuvenlax)
      } else if (fieldTypeDescriptor.getType().isCompositeType()) {
        size += sizeInBytes(row.getRow(fieldIndex));
      } else if (fieldTypeDescriptor.getType().isStringType()) {
        size += row.getString(fieldIndex).length() + 1;
      } else {
        Integer estimatedSize = ESTIMATED_FIELD_SIZES.get(fieldTypeDescriptor.getType());
        if (estimatedSize != null) {
          size += estimatedSize;
        } else {
          throw new IllegalStateException("Unexpected field type " + fieldTypeDescriptor.getType());
        }
      }
    }
    return size;
  }

  private long sizeInBytes;

  private RowSize(long sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }


  private static Integer bytes(int size) {
    return size / Byte.SIZE;
  }
}
