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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@Internal
public final class RowMessages {

  private RowMessages() {}

  public static <T> SimpleFunction<byte[], Row> bytesToRowFn(
      SchemaProvider schemaProvider,
      TypeDescriptor<T> typeDescriptor,
      ProcessFunction<byte[], ? extends T> fromBytesFn) {
    final SerializableFunction<T, Row> toRowFn =
        checkArgumentNotNull(schemaProvider.toRowFunction(typeDescriptor));
    return new BytesToRowFn<>(fromBytesFn, toRowFn);
  }

  public static <T> SimpleFunction<byte[], Row> bytesToRowFn(
      SchemaProvider schemaProvider, TypeDescriptor<T> typeDescriptor, Coder<? extends T> coder) {
    return bytesToRowFn(
        schemaProvider, typeDescriptor, bytes -> coder.decode(new ByteArrayInputStream(bytes)));
  }

  private static final class BytesToRowFn<T> extends SimpleFunction<byte[], Row> {

    private final ProcessFunction<byte[], ? extends T> fromBytesFn;
    private final SerializableFunction<T, Row> toRowFn;

    private BytesToRowFn(
        ProcessFunction<byte[], ? extends T> fromBytesFn, SerializableFunction<T, Row> toRowFn) {
      this.fromBytesFn = fromBytesFn;
      this.toRowFn = toRowFn;
    }

    @Override
    public Row apply(byte[] bytes) {
      final T message;
      try {
        message = fromBytesFn.apply(bytes);
      } catch (Exception e) {
        throw new IllegalStateException("Could not decode bytes as message", e);
      }
      return toRowFn.apply(message);
    }
  }

  public static <T> SimpleFunction<Row, byte[]> rowToBytesFn(
      SchemaProvider schemaProvider,
      TypeDescriptor<T> typeDescriptor,
      ProcessFunction<? super T, byte[]> toBytesFn) {
    final Schema schema = checkArgumentNotNull(schemaProvider.schemaFor(typeDescriptor));
    final SerializableFunction<Row, T> fromRowFn =
        checkArgumentNotNull(schemaProvider.fromRowFunction(typeDescriptor));
    toBytesFn = checkArgumentNotNull(toBytesFn);
    return new RowToBytesFn<>(schema, fromRowFn, toBytesFn);
  }

  public static <T> SimpleFunction<Row, byte[]> rowToBytesFn(
      SchemaProvider schemaProvider, TypeDescriptor<T> typeDescriptor, Coder<? super T> coder) {
    return rowToBytesFn(schemaProvider, typeDescriptor, message -> toBytes(coder, message));
  }

  private static <T> byte[] toBytes(Coder<? super T> coder, T message) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(message, out);
    return out.toByteArray();
  }

  private static final class RowToBytesFn<T> extends SimpleFunction<Row, byte[]> {

    private final Schema schema;
    private final SerializableFunction<Row, T> fromRowFn;
    private final ProcessFunction<? super T, byte[]> toBytesFn;

    private RowToBytesFn(
        Schema schema,
        SerializableFunction<Row, T> fromRowFn,
        ProcessFunction<? super T, byte[]> toBytesFn) {
      this.schema = schema;
      this.fromRowFn = fromRowFn;
      this.toBytesFn = toBytesFn;
    }

    @Override
    public byte[] apply(Row row) {
      if (!schema.equivalent(row.getSchema())) {
        row = switchFieldsOrder(row);
      }
      final T message = fromRowFn.apply(row);
      try {
        return toBytesFn.apply(message);
      } catch (Exception e) {
        throw new IllegalStateException("Could not encode message as bytes", e);
      }
    }

    private Row switchFieldsOrder(Row row) {
      Row.Builder convertedRow = Row.withSchema(schema);
      schema.getFields().forEach(field -> convertedRow.addValue(row.getValue(field.getName())));
      return convertedRow.build();
    }
  }
}
