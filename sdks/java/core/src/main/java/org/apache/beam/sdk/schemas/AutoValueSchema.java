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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.AutoValueUtils;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link SchemaProvider} for AutoValue classes. */
@Experimental(Kind.SCHEMAS)
public class AutoValueSchema extends GetterBasedSchemaProvider {
  /** {@link FieldValueTypeSupplier} that's based on AutoValue getters. */
  @VisibleForTesting
  public static class AbstractGetterTypeSupplier implements FieldValueTypeSupplier {
    public static final AbstractGetterTypeSupplier INSTANCE = new AbstractGetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      // If the generated class is passed in, we want to look at the base class to find the getters.
      Class<?> targetClass = AutoValueUtils.getBaseAutoValueClass(clazz);
      return ReflectUtils.getMethods(targetClass).stream()
          .filter(ReflectUtils::isGetter)
          // All AutoValue getters are marked abstract.
          .filter(m -> Modifier.isAbstract(m.getModifiers()))
          .filter(m -> !Modifier.isPrivate(m.getModifiers()))
          .filter(m -> !Modifier.isProtected(m.getModifiers()))
          .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
          .map(FieldValueTypeInformation::forGetter)
          .map(
              t -> {
                SchemaFieldName fieldName = t.getMethod().getAnnotation(SchemaFieldName.class);
                return (fieldName == null) ? t : t.withName(fieldName.value());
              })
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return JavaBeanUtils.getGetters(
        targetClass,
        schema,
        AbstractGetterTypeSupplier.INSTANCE,
        new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    return JavaBeanUtils.getFieldTypes(targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    // If a static method is marked with @SchemaCreate, use that.
    Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetClass);
    if (annotated != null) {
      return JavaBeanUtils.getStaticCreator(
          targetClass,
          annotated,
          schema,
          AbstractGetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    // Try to find a generated builder class. If one exists, use that to generate a
    // SchemaTypeCreator for creating AutoValue objects.
    SchemaUserTypeCreator creatorFactory =
        AutoValueUtils.getBuilderCreator(targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
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
  }

  @Nullable
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return JavaBeanUtils.schemaFromJavaBeanClass(
        typeDescriptor.getRawType(), AbstractGetterTypeSupplier.INSTANCE);
  }
}
