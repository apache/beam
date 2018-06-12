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

package org.apache.beam.sdk.values;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueGetterFactory;

public class RowWithGetters extends Row {
  private FieldValueGetterFactory fieldValueGetterFactory;
  private Object getterTarget;
  private List<FieldValueGetter> getters;

  RowWithGetters(Schema schema, FieldValueGetterFactory getterFactory, Object getterTarget) {
    super(schema);
    this.fieldValueGetterFactory = getterFactory;
    this.getterTarget = getterTarget;
    this.getters = fieldValueGetterFactory.createGetters(getterTarget.getClass());
  }

  @Nullable
  @Override
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> T getValue(int fieldIdx) {
    return (T) getters.get(fieldIdx).get(getterTarget);
  }


  @Override
  public int getFieldCount() {
    return getters.size();
  }

  @Override
  public List<Object> getValues() {
    return getters.stream()
        .map(g -> g.get(getterTarget))
        .collect(Collectors.toList());
  }

  public List<FieldValueGetter> getGetters() {
    return getters;
  }

  public Object getGetterTarget() {
    return getterTarget;
  }
}
