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
package org.apache.beam.sdk.io.gcp.bigtable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.joda.time.format.ISODateTimeFormat;

@SuppressWarnings("nullness")
public class BeamSchemaToBytesTransformers {

  @UnknownInitialization
  @FunctionalInterface
  public interface MyBiFunction<T, X, K> extends BiFunction<T, X, K> {

    @Override
    @Nullable
    K apply(T t, X u);

    @Override
    default <V> BiFunction<T, X, V> andThen(Function<? super K, ? extends V> after) {
      Objects.requireNonNull(after);
      return (T t, X u) -> after.apply(apply(t, u));
    }
  }

  private static byte[] toBytes(@Nullable Object obj) {
    if (obj == null) {
      return new byte[0];
    } else {
      return obj.toString().getBytes(StandardCharsets.UTF_8);
    }
  }

  public static byte[] encodeArrayField(Row row, Schema.Field field) {
    if (field.getType().getCollectionElementType().equals(Schema.FieldType.DATETIME)) {
      throw new UnsupportedOperationException("Datetime arrays are not supported!");
    } else {
      return Bytes.concat(
          new byte[] {(byte) '['},
          row.getArray(field.getName()).stream()
              .map(obj -> obj == null ? "" : obj.toString())
              .collect(Collectors.joining(","))
              .getBytes(StandardCharsets.UTF_8),
          new byte[] {(byte) ']'});
    }
  }

  @SuppressWarnings("DoNotCallSuggester")
  public static byte[] encodeRowField(Row row, Schema.Field field) {
    throw new UnsupportedOperationException("Nested fields are not supported!");
  }

  public static MyBiFunction<Row, Schema.Field, byte[]> getBytesEncoders(Schema.TypeName type) {
    switch (type) {
      case BYTES:
        return ((row, field) -> row.getBytes(field.getName()));
      case STRING:
        return ((row, field) -> toBytes(row.getString(field.getName())));
      case BOOLEAN:
        return ((row, field) -> toBytes(row.getBoolean(field.getName())));
      case DOUBLE:
        return ((row, field) -> toBytes(row.getDouble(field.getName())));
      case FLOAT:
        return ((row, field) -> toBytes(row.getFloat(field.getName())));
      case INT16:
        return ((row, field) -> toBytes(row.getInt16(field.getName())));
      case INT32:
        return ((row, field) -> toBytes(row.getInt32(field.getName())));
      case INT64:
        return ((row, field) -> toBytes(row.getInt64(field.getName())));
      case DECIMAL:
        return ((row, field) -> toBytes(row.getDecimal(field.getName())));
      case ARRAY:
        return BeamSchemaToBytesTransformers::encodeArrayField;
      case ROW:
        return BeamSchemaToBytesTransformers::encodeRowField;
      case DATETIME:
        return ((row, field) -> {
          if (row.getDateTime(field.getName()) == null) {
            return new byte[0];
          } else {
            return ISODateTimeFormat.basicDateTime()
                .print(row.getDateTime(field.getName()))
                .getBytes(StandardCharsets.UTF_8);
          }
        });
      default:
        return null;
    }
  }
}
