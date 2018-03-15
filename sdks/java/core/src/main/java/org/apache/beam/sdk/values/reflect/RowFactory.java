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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Schema;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Generates the code to create {@link Schema}s and {@link Row}s based on pojos.
 *
 * <p>Generated record types are cached in the instance of this factory.
 *
 * <p>At the moment single pojo class corresponds to single {@link Schema}.
 *
 * <p>Supported pojo getter types depend on types supported by the {@link RowTypeFactory}.
 * See {@link DefaultRowTypeFactory} for default implementation.
 */
@Internal
public class RowFactory {

  private RowTypeFactory rowTypeFactory;
  private final Map<Class, RowTypeGetters> rowTypesCache = new HashMap<>();
  private final List<GetterFactory> getterFactories;

  /**
   * Creates an instance of {@link RowFactory} using {@link DefaultRowTypeFactory}
   * and {@link GeneratedGetterFactory}.
   */
  public static RowFactory createDefault() {
    return new RowFactory();
  }

  /**
   * Create new instance based on default record type factory.
   *
   * <p>Use this to create instances of {@link Schema}.
   */
  private RowFactory() {
    this(new DefaultRowTypeFactory(), new GeneratedGetterFactory());
  }

  /**
   * Create new instance with custom record type factory.
   *
   * <p>For example this can be used to create BeamRecordSqlTypes instead of {@link Schema}.
   */
  RowFactory(RowTypeFactory rowTypeFactory, GetterFactory ... getterFactories) {
    this.rowTypeFactory = rowTypeFactory;
    this.getterFactories = Arrays.asList(getterFactories);
  }

  /**
   * Create a {@link Row} of the pojo.
   *
   * <p>This implementation copies the return values of the pojo getters into
   * the record fields on creation.
   *
   * <p>Currently all public getters are used to populate the record type and instance.
   *
   * <p>Field names for getters are stripped of the 'get' prefix.
   * For example record field 'name' will be generated for 'getName()' pojo method.
   */
  public Row create(Object pojo) {
    RowTypeGetters getters = getRecordType(pojo.getClass());
    List<Object> fieldValues = getFieldValues(getters.valueGetters(), pojo);
    return Row.withRowType(getters.rowType()).addValues(fieldValues).build();
  }

  private synchronized RowTypeGetters getRecordType(Class pojoClass) {
    if (rowTypesCache.containsKey(pojoClass)) {
      return rowTypesCache.get(pojoClass);
    }

    List<FieldValueGetter> fieldValueGetters = createGetters(pojoClass);
    Schema schema = rowTypeFactory.createRowType(fieldValueGetters);
    rowTypesCache.put(pojoClass, new RowTypeGetters(schema, fieldValueGetters));

    return rowTypesCache.get(pojoClass);
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
