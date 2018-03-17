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
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * {@link SchemaCoder} is used as the coder for types that have schemas registered.
 */
@Experimental
public class SchemaCoder<T> extends CustomCoder<T> {

  /**
   * Returns a {@link SchemaCoder} for the specified class. If no schema is registered for this
   * class, then throws {@link NoSuchSchemaException}.
   * TODO: In the future we might want to move schema lookup into coder inference, and have it
   * passed into the coder instead.
   */
  public static <T> SchemaCoder<T> of(
      Class<T> clazz,
      Schema schema,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {

  }

  /**
   * Returns a {@link SchemaCoder} for the specified type. If no schema is registered for this
   * class, then throws {@link NoSuchSchemaException}.
   */
  public static <T> SchemaCoder<T> of(
      TypeDescriptor<T> typeDescriptor,
      Schema schema,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {

  }


  /**
   * Returns the schema associated with this type.
   */
  public Schema getSchema() {

  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

  }
}
