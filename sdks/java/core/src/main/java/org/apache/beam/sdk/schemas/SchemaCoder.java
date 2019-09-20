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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** {@link SchemaCoder} is used as the coder for types that have schemas registered. */
@Experimental(Kind.SCHEMAS)
public class SchemaCoder<T> extends CustomCoder<T> {
  private final RowCoder rowCoder;
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<T, Row> toRowFunction;
  private final SerializableFunction<Row, T> fromRowFunction;

  private SchemaCoder(
      Schema schema,
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {
    checkArgument(
        !typeDescriptor.hasUnresolvedParameters(),
        "Cannot create SchemaCoder with a TypeDescriptor that has unresolved parameters: %s",
        typeDescriptor);
    this.toRowFunction = toRowFunction;
    this.fromRowFunction = fromRowFunction;
    this.typeDescriptor = typeDescriptor;
    this.rowCoder = RowCoder.of(schema);
  }

  /**
   * Returns a {@link SchemaCoder} for the specified class. If no schema is registered for this
   * class, then throws {@link NoSuchSchemaException}.
   */
  public static <T> SchemaCoder<T> of(
      Schema schema,
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {
    return new SchemaCoder<>(schema, typeDescriptor, toRowFunction, fromRowFunction);
  }

  /** Returns a {@link SchemaCoder} for {@link Row} classes. */
  public static SchemaCoder<Row> of(Schema schema) {
    return new SchemaCoder<>(schema, TypeDescriptors.rows(), identity(), identity());
  }

  /** Returns the schema associated with this type. */
  public Schema getSchema() {
    return rowCoder.getSchema();
  }

  /** Returns the toRow conversion function. */
  public SerializableFunction<Row, T> getFromRowFunction() {
    return fromRowFunction;
  }

  /** Returns the fromRow conversion function. */
  public SerializableFunction<T, Row> getToRowFunction() {
    return toRowFunction;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    rowCoder.encode(toRowFunction.apply(value), outStream);
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    return fromRowFunction.apply(rowCoder.decode(inStream));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    rowCoder.verifyDeterministic();
  }

  @Override
  public boolean consistentWithEquals() {
    return rowCoder.consistentWithEquals();
  }

  @Override
  public String toString() {
    return "SchemaCoder: " + rowCoder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaCoder<?> that = (SchemaCoder<?>) o;
    return rowCoder.equals(that.rowCoder)
        && typeDescriptor.equals(that.typeDescriptor)
        && toRowFunction.equals(that.toRowFunction)
        && fromRowFunction.equals(that.fromRowFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCoder, toRowFunction, fromRowFunction);
  }

  private static RowIdentity identity() {
    return new RowIdentity();
  }

  private static class RowIdentity implements SerializableFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      return input;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return this.typeDescriptor;
  }
}
