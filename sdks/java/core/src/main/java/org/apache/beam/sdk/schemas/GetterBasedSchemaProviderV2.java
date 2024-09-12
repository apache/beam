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

import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A newer version of {@link GetterBasedSchemaProvider}, which works with {@link TypeDescriptor}s,
 * and which by default delegates the old, {@link Class} based methods, to the new ones.
 */
@SuppressWarnings("rawtypes")
public abstract class GetterBasedSchemaProviderV2 extends GetterBasedSchemaProvider {
  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return fieldValueGetters(TypeDescriptor.of(targetClass), schema);
  }

  @Override
  public abstract List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema);

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    return fieldValueTypeInformations(TypeDescriptor.of(targetClass), schema);
  }

  @Override
  public abstract List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema);

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    return schemaTypeCreator(TypeDescriptor.of(targetClass), schema);
  }

  @Override
  public abstract SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema);
}
