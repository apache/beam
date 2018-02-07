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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

/**
 * {@link KnownSize} implementation to estimate the size of a {@link Row},
 * similar to Java model. NexmarkLauncher/Queries infrastructure expects the events to
 * be able to quickly provide the estimates of their sizes.
 *
 * <p>The {@link Row} size is calculated at creation time.
 *
 * <p>Field sizes are sizes of Java types described in {@link RowSqlType}. Except strings,
 * which are assumed to be taking 1-byte per character plus 1 byte size.
 */
public class RowSize implements KnownSize {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  public static final Coder<RowSize> CODER = new CustomCoder<RowSize>() {
    @Override
    public void encode(RowSize rowSize, OutputStream outStream)
        throws CoderException, IOException {

      LONG_CODER.encode(rowSize.sizeInBytes(), outStream);
    }

    @Override
    public RowSize decode(InputStream inStream) throws CoderException, IOException {
      return new RowSize(LONG_CODER.decode(inStream));
    }
  };

  private static final Map<SqlTypeCoder, Integer> ESTIMATED_FIELD_SIZES =
      ImmutableMap.<SqlTypeCoder, Integer>builder()
          .put(SqlTypeCoders.TINYINT, bytes(Byte.SIZE))
          .put(SqlTypeCoders.SMALLINT, bytes(Short.SIZE))
          .put(SqlTypeCoders.INTEGER, bytes(Integer.SIZE))
          .put(SqlTypeCoders.BIGINT, bytes(Long.SIZE))
          .put(SqlTypeCoders.FLOAT, bytes(Float.SIZE))
          .put(SqlTypeCoders.DOUBLE, bytes(Double.SIZE))
          .put(SqlTypeCoders.DECIMAL, 32)
          .put(SqlTypeCoders.BOOLEAN, 1)
          .put(SqlTypeCoders.TIME, bytes(Long.SIZE))
          .put(SqlTypeCoders.DATE, bytes(Long.SIZE))
          .put(SqlTypeCoders.TIMESTAMP, bytes(Long.SIZE))
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
    RowType rowType = row.getRowType();
    long size = 1; // nulls bitset

    for (int fieldIndex = 0; fieldIndex < rowType.getFieldCount(); fieldIndex++) {
      Coder fieldType = rowType.getFieldCoder(fieldIndex);

      Integer estimatedSize = ESTIMATED_FIELD_SIZES.get(fieldType);

      if (estimatedSize != null) {
        size += estimatedSize;
        continue;
      }

      if (isString(fieldType)) {
        size += row.getString(fieldIndex).length() + 1;
        continue;
      }

      throw new IllegalStateException("Unexpected field type " + fieldType);
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

  private static boolean isString(Coder fieldType) {
    return SqlTypeCoders.CHAR.equals(fieldType)
        || SqlTypeCoders.VARCHAR.equals(fieldType);
  }

  private static Integer bytes(int size) {
    return size / Byte.SIZE;
  }
}
