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

import static org.apache.beam.sdk.values.reflect.ReflectionUtils.getPublicGetters;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.sdk.values.RowType;

/**
 * Factory to wrap calls to pojo getters into instances of {@link FieldValueGetter}
 * using reflection.
 *
 * <p>Returns instances of {@link FieldValueGetter}s backed getter methods of a pojo class.
 * Getters are invoked using {@link java.lang.reflect.Method#invoke(Object, Object...)}
 * from {@link FieldValueGetter#get(Object)}.
 *
 * <p>Caching is not handled at this level, {@link RowFactory} should cache getters
 * for each {@link RowType}.
 */
class ReflectionGetterFactory implements GetterFactory {

  /**
   * Returns a list of {@link FieldValueGetter}s.
   * One for each public getter of the {@code pojoClass}.
   */
  @Override
  public List<FieldValueGetter> generateGetters(Class pojoClass) {
    ImmutableList.Builder<FieldValueGetter> getters = ImmutableList.builder();

    for (Method getterMethod : getPublicGetters(pojoClass)) {
      getters.add(new ReflectionGetter(getterMethod));
    }

    return getters.build();
  }
}
