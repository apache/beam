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
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.AutoValueUtils;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link SchemaProvider} for AutoValue classes. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class AutoValueSchema extends GetterBasedSchemaProviderV2 {
  /** {@link FieldValueTypeSupplier} that's based on AutoValue getters. */
  @VisibleForTesting
  public static class AbstractGetterTypeSupplier implements FieldValueTypeSupplier {
    public static final AbstractGetterTypeSupplier INSTANCE = new AbstractGetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {

      // If the generated class is passed in, we want to look at the base class to find the getters.
      TypeDescriptor<?> targetTypeDescriptor = AutoValueUtils.getBaseAutoValueClass(typeDescriptor);

      List<Method> methods =
          ReflectUtils.getMethods(targetTypeDescriptor.getRawType()).stream()
              .filter(ReflectUtils::isGetter)
              // All AutoValue getters are marked abstract.
              .filter(m -> Modifier.isAbstract(m.getModifiers()))
              .filter(m -> !Modifier.isPrivate(m.getModifiers()))
              .filter(m -> !Modifier.isProtected(m.getModifiers()))
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .collect(Collectors.toList());
      List<FieldValueTypeInformation> types = Lists.newArrayListWithCapacity(methods.size());
      Map<Type, Type> boundTypes = ReflectUtils.getAllBoundTypes(typeDescriptor);
      for (int i = 0; i < methods.size(); ++i) {
        types.add(FieldValueTypeInformation.forGetter(methods.get(i), i, boundTypes));
      }
      types.sort(Comparator.comparing(FieldValueTypeInformation::getNumber));
      validateFieldNumbers(types);
      return types;
    }
  }

  private static void validateFieldNumbers(List<FieldValueTypeInformation> types) {
    for (int i = 0; i < types.size(); ++i) {
      FieldValueTypeInformation type = types.get(i);
      @javax.annotation.Nullable Integer number = type.getNumber();
      if (number == null) {
        throw new RuntimeException("Unexpected null number for " + type.getName());
      }
      Preconditions.checkState(
          number == i,
          "Expected field number "
              + i
              + " for field + "
              + type.getName()
              + " instead got "
              + number);
    }
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return JavaBeanUtils.getGetters(
        targetTypeDescriptor,
        schema,
        AbstractGetterTypeSupplier.INSTANCE,
        new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return JavaBeanUtils.getFieldTypes(
        targetTypeDescriptor, schema, AbstractGetterTypeSupplier.INSTANCE);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    // If a static method is marked with @SchemaCreate, use that.
    Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetTypeDescriptor.getRawType());
    if (annotated != null) {
      return JavaBeanUtils.getStaticCreator(
          targetTypeDescriptor,
          annotated,
          schema,
          AbstractGetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    // Try to find a generated builder class. If one exists, use that to generate a
    // SchemaTypeCreator for creating AutoValue objects.
    SchemaUserTypeCreator creatorFactory =
        AutoValueUtils.getBuilderCreator(
            targetTypeDescriptor.getRawType(), schema, AbstractGetterTypeSupplier.INSTANCE);
    if (creatorFactory != null) {
      return creatorFactory;
    }

    // If there is no builder, there should be a package-private constructor in the generated
    // class. Use that for creating AutoValue objects.
    creatorFactory =
        AutoValueUtils.getConstructorCreator(
            targetTypeDescriptor, schema, AbstractGetterTypeSupplier.INSTANCE);
    if (creatorFactory == null) {
      throw new RuntimeException(
          "Could not find a way to create AutoValue class " + targetTypeDescriptor);
    }

    return creatorFactory;
  }

  @Override
  public <T> @Nullable Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    Map<Type, Type> boundTypes = ReflectUtils.getAllBoundTypes(typeDescriptor);
    return JavaBeanUtils.schemaFromJavaBeanClass(
        typeDescriptor, AbstractGetterTypeSupplier.INSTANCE, boundTypes);
  }
}
