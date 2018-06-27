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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Generates the code to create {@link Schema}s and {@link Row}s based on pojos.
 *
 * <p>Generated record types are cached in the instance of this factory.
 *
 * <p>At the moment single pojo class corresponds to single {@link Schema}.
 *
 * <p>Supported pojo getter types depend on types supported by the {@link SchemaFactory}. See {@link
 * DefaultSchemaFactory} for default implementation.
 */
@Internal
public class RowFactory implements Serializable {

  private SchemaFactory schemaFactory;
  private final List<GetterFactory> getterFactories;
  private transient Map<Class, SchemaGetters> schemaCache;

  /**
   * Creates an instance of {@link RowFactory} using {@link DefaultSchemaFactory} and {@link
   * GeneratedGetterFactory}.
   */
  public static RowFactory createDefault() {
    return withSchemaFactory(new DefaultSchemaFactory());
  }

  /**
   * Creates an instance of {@link RowFactory} using provided {@link SchemaFactory} and {@link
   * GeneratedGetterFactory}.
   */
  public static RowFactory withSchemaFactory(SchemaFactory schemaFactory) {
    return of(schemaFactory, new GeneratedGetterFactory());
  }

  /**
   * Creates an instance of {@link RowFactory} using provided {@link SchemaFactory} and {@link
   * GetterFactory}.
   */
  public static RowFactory of(SchemaFactory schemaFactory, GetterFactory getterFactory) {
    return new RowFactory(schemaFactory, getterFactory);
  }

  /**
   * Create new instance with custom record type factory.
   *
   * <p>For example this can be used to create BeamRecordSqlTypes instead of {@link Schema}.
   */
  RowFactory(SchemaFactory schemaFactory, GetterFactory... getterFactories) {
    this.schemaFactory = schemaFactory;
    this.getterFactories = Arrays.asList(getterFactories);
  }

  public <T> Schema getSchema(Class<T> elementType) {
    return getRecordType(elementType).schema();
  }

  /**
   * Create a {@link Row} of the pojo.
   *
   * <p>This implementation copies the return values of the pojo getters into the record fields on
   * creation.
   *
   * <p>Currently all public getters are used to populate the record type and instance.
   *
   * <p>Field names for getters are stripped of the 'get' prefix. For example record field 'name'
   * will be generated for 'getName()' pojo method.
   */
  public <T> Row create(T pojo) {
    SchemaGetters getters = getRecordType(pojo.getClass());
    List<Object> fieldValues = getFieldValues(getters.valueGetters(), pojo);
    return Row.withSchema(getters.schema()).addValues(fieldValues).build();
  }

  private synchronized SchemaGetters getRecordType(Class pojoClass) {
    if (schemaCache == null) {
      schemaCache = new HashMap<>();
    }

    if (schemaCache.containsKey(pojoClass)) {
      return schemaCache.get(pojoClass);
    }

    List<FieldValueGetter> fieldValueGetters = createGetters(pojoClass);
    Schema schema = schemaFactory.createSchema(fieldValueGetters);
    schemaCache.put(pojoClass, new SchemaGetters(schema, fieldValueGetters));

    return schemaCache.get(pojoClass);
  }

  private List<FieldValueGetter> createGetters(Class pojoClass) {
    ImmutableList.Builder<FieldValueGetter> getters = ImmutableList.builder();

    for (GetterFactory getterFactory : getterFactories) {
      getters.addAll(getterFactory.generateGetters(pojoClass));
    }

    return getters.build();
  }

  private List<Object> getFieldValues(List<FieldValueGetter> fieldValueGetters, Object pojo) {
    ImmutableList.Builder<Object> builder = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      builder.add(fieldValueGetter.get(pojo));
    }

    return builder.build();
  }
}
