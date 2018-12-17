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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.annotations.FieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.AutoValueUtils;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AutoValueSchema extends GetterBasedSchemaProvider {
  public static class AbstractGetterTypeSupplier implements FieldValueTypeSupplier {
    public static final AbstractGetterTypeSupplier INSTANCE = new AbstractGetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz, @Nullable Schema schema) {
      // If the generated class is passed in, we want to look at the base class to find the getters.
      Class<?> targetClass = AutoValueUtils.getBaseAutoValueClass(clazz);
      List<FieldValueTypeInformation> types =
          ReflectUtils.getMethods(targetClass)
              .stream()
              .filter(ReflectUtils::isGetter)
              // All AutoValue getters are marked abstract.
              .filter(m -> Modifier.isAbstract(m.getModifiers()))
              .filter(m -> !Modifier.isPrivate(m.getModifiers()))
              .filter(m -> !Modifier.isProtected(m.getModifiers()))
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .map(FieldValueTypeInformation::forGetter)
              .map(
                  t -> {
                    FieldName fieldName = t.getMethod().getAnnotation(FieldName.class);
                    return (fieldName != null) ? t.withName(fieldName.value()) : t;
                  })
              .collect(Collectors.toList());
      return (schema != null) ? StaticSchemaInference.sortBySchema(types, schema) : types;
    }
  }

  @Override
  FieldValueGetterFactory fieldValueGetterFactory() {
    return (Class<?> targetClass, Schema schema) ->
        JavaBeanUtils.getGetters(targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
  }

  @Override
  FieldValueTypeInformationFactory fieldValueTypeInformationFactory() {
    return (Class<?> targetClass, Schema schema) ->
        JavaBeanUtils.getFieldTypes(targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
  }

  @Override
  UserTypeCreatorFactory schemaTypeCreatorFactory() {
    return (Class<?> targetClass, Schema schema) -> {
      // If a static method is marked with @SchemaCreate, use that.
      Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetClass);
      if (annotated != null) {
        return JavaBeanUtils.getStaticCreator(
            targetClass, annotated, schema, AbstractGetterTypeSupplier.INSTANCE);
      }

      // Try to find a generated builder class. If one exists, use that to generate a
      // SchemaTypeCreator for creating AutoValue objects.
      SchemaUserTypeCreator creatorFactory =
          AutoValueUtils.getBuilderCreator(
              targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
      if (creatorFactory != null) {
        return creatorFactory;
      }

      // If there is no builder, there should be a package-private constructor in the generated
      // class. Use that for creating AutoValue objects.
      creatorFactory =
          AutoValueUtils.getConstructorCreator(
              targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
      if (creatorFactory == null) {
        throw new RuntimeException("Could not find a way to create AutoValue class " + targetClass);
      }

      return creatorFactory;
    };
  }

  @Nullable
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return JavaBeanUtils.schemaFromJavaBeanClass(
        typeDescriptor.getRawType(), AbstractGetterTypeSupplier.INSTANCE);
  }
}
