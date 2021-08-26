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

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Concrete implementations of this class allow creation of schema service objects that vend a
 * {@link Schema} for a specific type. One example use: creating a {@link SchemaProvider} that
 * contacts an external schema-registry service to determine the schema for a type.
 */
@Experimental(Kind.SCHEMAS)
public interface SchemaProvider extends Serializable {

  /** Lookup a schema for the given type. If no schema exists, returns null. */
  @Nullable
  <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor);

  /**
   * Given a type, return a function that converts that type to a {@link Row} object If no schema
   * exists, returns null.
   */
  @Nullable
  <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor);

  /**
   * Given a type, returns a function that converts from a {@link Row} object to that type. If no
   * schema exists, returns null.
   */
  @Nullable
  <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor);
}
