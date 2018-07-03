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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.common.ReflectHelpers.ObjectsClassComparator;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaRegistry} allows registering {@link Schema}s for a given Java {@link Class} or a
 * {@link TypeDescriptor}.
 *
 * <p>Types registered in a pipeline's schema registry will automatically be discovered by any
 * {@link org.apache.beam.sdk.values.PCollection} that uses {@link SchemaCoder}. This allows users
 * to write pipelines in terms of their own Java types, yet still register schemas for these types.
 *
 * <p>TODO: Provide support for schemas registered via a ServiceLoader interface. This will allow
 * optional modules to register schemas as well.
 */
@Experimental(Kind.SCHEMAS)
public class SchemaRegistry {
  private static final List<SchemaProvider> REGISTERED_SCHEMA_PROVIDERS;

  private static class SchemaEntry<T> {
    private final Schema schema;
    private final SerializableFunction<T, Row> toRow;
    private final SerializableFunction<Row, T> fromRow;

    SchemaEntry(
        Schema schema, SerializableFunction<T, Row> toRow, SerializableFunction<Row, T> fromRow) {
      this.schema = schema;
      this.toRow = toRow;
      this.fromRow = fromRow;
    }
  }

  private final Map<TypeDescriptor, SchemaEntry> entries = Maps.newHashMap();
  private final ArrayDeque<SchemaProvider> providers;

  private SchemaRegistry() {
    providers = new ArrayDeque<>(REGISTERED_SCHEMA_PROVIDERS);
  }

  public static SchemaRegistry createDefault() {
    return new SchemaRegistry();
  }

  /** Register a schema for a specific {@link Class} type. */
  public <T> void registerSchemaForClass(
      Class<T> clazz,
      Schema schema,
      SerializableFunction<T, Row> toRow,
      SerializableFunction<Row, T> fromRow) {
    registerSchemaForType(TypeDescriptor.of(clazz), schema, toRow, fromRow);
  }

  /** Register a schema for a specific {@link TypeDescriptor} type. */
  public <T> void registerSchemaForType(
      TypeDescriptor<T> type,
      Schema schema,
      SerializableFunction<T, Row> toRow,
      SerializableFunction<Row, T> fromRow) {
    entries.put(type, new SchemaEntry<>(schema, toRow, fromRow));
  }

  /**
   * Register a {@link SchemaProvider}.
   *
   * <p>A {@link SchemaProvider} allows for deferred lookups of per-type schemas. This can be used
   * when schemas are registered in an external service. The SchemaProvider will lookup the type in
   * the external service and return the correct {@link Schema}.
   */
  public void registerSchemaProvider(SchemaProvider schemaProvider) {
    providers.addFirst(schemaProvider);
  }

  /**
   * Get a schema for a given {@link Class} type. If no schema exists, throws {@link
   * NoSuchSchemaException}.
   */
  public <T> Schema getSchema(Class<T> clazz) throws NoSuchSchemaException {
    return getSchema(TypeDescriptor.of(clazz));
  }

  private <ReturnT> ReturnT getProviderResult(Function<SchemaProvider, ReturnT> f)
      throws NoSuchSchemaException {
    for (SchemaProvider provider : providers) {
      ReturnT result = f.apply(provider);
      if (result != null) {
        return result;
      }
    }
    throw new NoSuchSchemaException();
  }

  /**
   * Retrieve a schema for a given {@link TypeDescriptor} type. If no schema exists, throws {@link
   * NoSuchSchemaException}.
   */
  public <T> Schema getSchema(TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {
    SchemaEntry entry = entries.get(typeDescriptor);
    if (entry != null) {
      return entry.schema;
    }
    return getProviderResult((SchemaProvider p) -> p.schemaFor(typeDescriptor));
  }

  /** Rerieve the function that converts an object of the specified type to a {@link Row} object. */
  public <T> SerializableFunction<T, Row> getToRowFunction(Class<T> clazz)
      throws NoSuchSchemaException {
    return getToRowFunction(TypeDescriptor.of(clazz));
  }

  /** Rerieve the function that converts an object of the specified type to a {@link Row} object. */
  public <T> SerializableFunction<T, Row> getToRowFunction(TypeDescriptor<T> typeDescriptor)
      throws NoSuchSchemaException {
    SchemaEntry entry = entries.get(typeDescriptor);
    if (entry != null) {
      return entry.toRow;
    }
    return getProviderResult((SchemaProvider p) -> p.toRowFunction(typeDescriptor));
  }

  /** Rerieve the function that converts a {@link Row} object to an object of the specified type. */
  public <T> SerializableFunction<Row, T> getFromRowFunction(Class<T> clazz)
      throws NoSuchSchemaException {
    return getFromRowFunction(TypeDescriptor.of(clazz));
  }

  /** Rerieve the function that converts a {@link Row} object to an object of the specified type. */
  public <T> SerializableFunction<Row, T> getFromRowFunction(TypeDescriptor<T> typeDescriptor)
      throws NoSuchSchemaException {
    SchemaEntry entry = entries.get(typeDescriptor);
    if (entry != null) {
      return entry.fromRow;
    }
    return getProviderResult((SchemaProvider p) -> p.fromRowFunction(typeDescriptor));
  }

  static {
    // find all statically-registered SchemaProviders.
    List<SchemaProvider> providersToRegister = Lists.newArrayList();
    Set<SchemaProviderRegistrar> registrars = Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    // Find all SchemaProviderRegistrar classes that are registered as service loaders
    // (usually using the @AutoService annotation).
    registrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(SchemaProviderRegistrar.class, ReflectHelpers.findClassLoader())));
    // Load all SchemaProviders that are registered using the @DefaultSchema annotation.
    providersToRegister.addAll(
        new DefaultSchema.DefaultSchemaProviderRegistrar().getSchemaProviders());
    for (SchemaProviderRegistrar registrar : registrars) {
      providersToRegister.addAll(registrar.getSchemaProviders());
    }
    REGISTERED_SCHEMA_PROVIDERS = ImmutableList.copyOf(providersToRegister);
  }
}
