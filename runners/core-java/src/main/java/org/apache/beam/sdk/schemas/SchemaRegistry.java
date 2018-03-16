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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaRegistry} allows registering {@link Schema}s for a given Java {@link Class} or a
 *{@link TypeDescriptor}.
 *
 * <p>Types registered in a pipeline's schema registry will automatically be discovered by any
 * {@ink PCollection} that uses {@link SchemaCoder}. This allows users to write pipelines in terms
 * of their own Java types, yet still register schemas for these types.
 */
@Experimental
public class SchemaRegistry {

  /**
   * Register a schema for a specific {@link Class} type.
   */
  public <T> void registerSchemaForClass(Class<T> clazz,
      Schema schema,
      SerializableFunction<T, Row> toRow,
      SerializableFunction<Row, T> fromRow) {

  }

  /**
   * Register a schema for a specific {@link TypeDescriptor} type.
   */
  public <T> void registerSchemaForType(TypeDescriptor<T> type,
      Schema schema,
      SerializableFunction<T, Row> toRow,
      SerializableFunction<Row, T> fromRow) {

  }

  /**
   * Register a {@link SchemaProvider}.
   *
   * <o>A {@link SchemaProvider} allows for deferred lookups of per-type schemas. This can be
   * used when scheams are registered in an external service. The SchemaProvider will lookup the
   * type in the external service and return the correct {@link Schema}.
   */
  public void registerSchemaProvider(SchemaProvider schemaProvider) {

  }

  /**
   * Get a schema for a given {@link Class} type. If no schema exists, throws
   * @link NoSuchSchemaException}.
   */
  public <T> Schema getSchema(Class<T> clazz) throws NoSuchSchemaException {

  }

  /**
   * Retrieve a schema for a given {@link TypeDescriptor} type. If no schema exists, throws
   * {@link NoSuchSchemaException}.
   */
  public <T> Schema getSchema(TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {

  }

  /**
   * Rerieve the function that converts an object of the specified type to a {@link Row} object.
   */
  public <T> SerializableFunction<T, Row> getToRowFunction(
      Class<T> clazz) throws NoSuchSchemaException {

  }

  /**
   * Rerieve the function that converts an object of the specified type to a {@link Row} object.
   */
  public <T> SerializableFunction<Row, T> getToRowFunction(
      TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {

  }

  /**
   * Rerieve the function that converts a {@link Row} object to an object of the specified type.
   */
  public <T> SerializableFunction<T, Row> getFromRowFunction(
      Class<T> clazz) throws NoSuchSchemaException {

  }

  /**
   * Rerieve the function that converts a {@link Row} object to an object of the specified type.
   */
  public <T> SerializableFunction<T, Row> getFromRowFunction(
      TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {

  }
}
