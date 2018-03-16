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

import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaRegistry} allows registering {@link Schema}s for a given Java {@link Class} or a
 *{@link TypeDescriptor}.
 *
 * <p>Types registered in a pipeline's schema registry will automatically be discovered by any
 * {@ink PCollection} that uses {@link SchemaCoder}. This allows users to write pipelines in terms
 * of their own Java types, yet still register schemas for these types.
 */
public class SchemaRegistry {

  /**
   * Register a schema for a specific {@link Class} type.
   */
  public void registerSchemaForClass(Class<?> clazz, Schema schema) {

  }

  /**
   * Register a schema for a specific {@link TypeDescriptor} type.
   */
  public void registerSchemaForType(TypeDescriptor<?> type, Schema schema) {

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
   * Get a schema for a given {@link TypeDescriptor} type. If no schema exists, throws
   * {@link NoSuchSchemaException}.
   */
  public <T> Schema getSchema(TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {

  }
}
