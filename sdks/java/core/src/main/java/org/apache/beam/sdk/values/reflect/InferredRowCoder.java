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

package org.apache.beam.sdk.values.reflect;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Provides interface to create {@link Schema} and then {@link Row} instances
 * based on the element type.
 *
 * <p>Relies on delegate {@code elementCoder} to encode original elements.
 */
@AutoValue
@Experimental
public abstract class InferredRowCoder<T> extends CustomCoder<T> {

  abstract RowFactory rowFactory();
  abstract Coder<T> delegateCoder();
  abstract Class<T> elementType();

  /**
   * Creates a {@link InferredRowCoder} delegating to the {@link SerializableCoder}
   * for encoding the {@link PCollection} elements.
   */
  public static <W extends Serializable> InferredRowCoder<W> ofSerializable(Class<W> elementType) {
    return of(elementType, SerializableCoder.of(elementType));
  }

  /**
   * Creates a {@link InferredRowCoder} delegating to the {@code elementCoder}
   * for encoding the {@link PCollection} elements.
   */
  public static <W> InferredRowCoder<W> of(Class<W> elementType, Coder<W> elementCoder) {
    return InferredRowCoder.<W>builder()
        .setRowFactory(RowFactory.createDefault())
        .setDelegateCoder(elementCoder)
        .setElementType(elementType)
        .build();
  }

  /**
   * Returns a {@link InferredRowCoder} with row type factory overridden by {@code rowTypeFactory}.
   */
  public InferredRowCoder<T> withRowTypeFactory(RowTypeFactory rowTypeFactory) {
    return toBuilder().setRowFactory(RowFactory.withRowTypeFactory(rowTypeFactory)).build();
  }

  static <W> Builder<W> builder() {
    return new AutoValue_InferredRowCoder.Builder<W>();
  }

  abstract Builder<T> toBuilder();

  public Schema rowType() {
    return rowFactory().getRowType(elementType());
  }

  public RowCoder rowCoder() {
    return rowType().getRowCoder();
  }

  public Row createRow(T element) {
    return rowFactory().create(element);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    delegateCoder().encode(value, outStream);
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    return delegateCoder().decode(inStream);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegateCoder().verifyDeterministic();
  }

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setRowFactory(RowFactory rowFactory);
    abstract Builder<T> setElementType(Class<T> clazz);
    abstract Builder<T> setDelegateCoder(Coder<T> coder);

    abstract InferredRowCoder<T> build();
  }
}
