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
package org.apache.beam.sdk.schemas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

class SchemaCoderHelpers {
  // This contains a map of primitive types to their coders.
  private static final Map<TypeName, Coder> CODER_MAP =
      ImmutableMap.<TypeName, Coder>builder()
          .put(TypeName.BYTE, ByteCoder.of())
          .put(TypeName.BYTES, ByteArrayCoder.of())
          .put(TypeName.INT16, BigEndianShortCoder.of())
          .put(TypeName.INT32, VarIntCoder.of())
          .put(TypeName.INT64, VarLongCoder.of())
          .put(TypeName.DECIMAL, BigDecimalCoder.of())
          .put(TypeName.FLOAT, FloatCoder.of())
          .put(TypeName.DOUBLE, DoubleCoder.of())
          .put(TypeName.STRING, StringUtf8Coder.of())
          // DATETIME is an alias for logical millis_instant, encoded as an INT64 millis since
          // epoch.
          .put(
              TypeName.DATETIME,
              DelegateCoder.<Instant, Long>of(
                  VarLongCoder.of(), Instant::getMillis, Instant::ofEpochMilli))
          .put(TypeName.BOOLEAN, BooleanCoder.of())
          .build();

  private static class LogicalTypeCoder<InputT, BaseT> extends Coder<InputT> {
    private final LogicalType<InputT, BaseT> logicalType;
    private final Coder<BaseT> baseTypeCoder;
    private final boolean isDateTime;

    LogicalTypeCoder(LogicalType<InputT, BaseT> logicalType, Coder baseTypeCoder) {
      this.logicalType = logicalType;
      this.baseTypeCoder = baseTypeCoder;
      this.isDateTime = logicalType.getBaseType().equals(FieldType.DATETIME);
    }

    @Override
    public void encode(InputT value, OutputStream outStream) throws CoderException, IOException {
      BaseT baseType = logicalType.toBaseType(value);
      if (isDateTime) {
        baseType = (BaseT) ((ReadableInstant) baseType).toInstant();
      }
      baseTypeCoder.encode(baseType, outStream);
    }

    @Override
    public InputT decode(InputStream inStream) throws CoderException, IOException {
      BaseT baseType = baseTypeCoder.decode(inStream);
      return logicalType.toInputType(baseType);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      baseTypeCoder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      // we can't assume that InputT is consistent with equals.
      // TODO: We should plumb this through to logical types.
      return false;
    }

    @Override
    public Object structuralValue(InputT value) {
      if (baseTypeCoder.consistentWithEquals()) {
        return logicalType.toBaseType(value);
      } else {
        return baseTypeCoder.structuralValue(logicalType.toBaseType(value));
      }
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(InputT value) {
      return baseTypeCoder.isRegisterByteSizeObserverCheap(logicalType.toBaseType(value));
    }

    @Override
    public void registerByteSizeObserver(InputT value, ElementByteSizeObserver observer)
        throws Exception {
      baseTypeCoder.registerByteSizeObserver(logicalType.toBaseType(value), observer);
    }
  }

  /** Returns the coder used for a given primitive type. */
  public static <T> Coder<T> coderForFieldType(FieldType fieldType) {
    Coder<T> coder;
    switch (fieldType.getTypeName()) {
      case ROW:
        coder = (Coder<T>) SchemaCoder.of(fieldType.getRowSchema());
        break;
      case ARRAY:
        coder = (Coder<T>) ListCoder.of(coderForFieldType(fieldType.getCollectionElementType()));
        break;
      case ITERABLE:
        coder =
            (Coder<T>) IterableCoder.of(coderForFieldType(fieldType.getCollectionElementType()));
        break;
      case MAP:
        coder =
            (Coder<T>)
                MapCoder.of(
                    coderForFieldType(fieldType.getMapKeyType()),
                    coderForFieldType(fieldType.getMapValueType()));
        break;
      case LOGICAL_TYPE:
        coder =
            new LogicalTypeCoder(
                fieldType.getLogicalType(),
                coderForFieldType(fieldType.getLogicalType().getBaseType()));
        break;
      default:
        coder = (Coder<T>) CODER_MAP.get(fieldType.getTypeName());
    }
    Preconditions.checkNotNull(coder, "Unexpected field type " + fieldType.getTypeName());
    if (fieldType.getNullable()) {
      coder = NullableCoder.of(coder);
    }
    return coder;
  }
}
