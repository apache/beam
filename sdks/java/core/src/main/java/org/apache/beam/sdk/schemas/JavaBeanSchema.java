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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SchemaProvider} for Java Bean objects.
 *
 * <p>This provider finds (recursively) all public getters and setters in a Java object, and creates
 * schemas and rows that bind to those fields. The field order in the schema is not guaranteed to
 * match the method order in the class. The Java object is expected to have implemented a correct
 * .equals() and .hashCode methods The equals method must be completely determined by the schema
 * fields. i.e. if the object has hidden fields that are not reflected in the schema but are
 * compared in equals, then results will be incorrect.
 *
 * <p>TODO: Validate equals() method is provided, and if not generate a "slow" equals method based
 * on the schema.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class JavaBeanSchema extends GetterBasedSchemaProviderV2 {
  /** {@link FieldValueTypeSupplier} that's based on getter methods. */
  @VisibleForTesting
  public static class GetterTypeSupplier implements FieldValueTypeSupplier {
    public static final GetterTypeSupplier INSTANCE = new GetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      List<Method> methods =
          ReflectUtils.getMethods(typeDescriptor.getRawType()).stream()
              .filter(ReflectUtils::isGetter)
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .collect(Collectors.toList());
      List<FieldValueTypeInformation> types = Lists.newArrayListWithCapacity(methods.size());
      for (int i = 0; i < methods.size(); ++i) {
        types.add(FieldValueTypeInformation.forGetter(methods.get(i), i));
      }
      types.sort(Comparator.comparing(FieldValueTypeInformation::getNumber));
      validateFieldNumbers(types);
      return types;
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
                + " for field: "
                + type.getName()
                + " instead got "
                + number);
      }
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj != null && this.getClass() == obj.getClass();
    }
  }

  /** {@link FieldValueTypeSupplier} that's based on setter methods. */
  @VisibleForTesting
  public static class SetterTypeSupplier implements FieldValueTypeSupplier {
    private static final SetterTypeSupplier INSTANCE = new SetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      return ReflectUtils.getMethods(typeDescriptor.getRawType()).stream()
          .filter(ReflectUtils::isSetter)
          .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
          .map(FieldValueTypeInformation::forSetter)
          .map(
              t -> {
                if (t.getMethod().getAnnotation(SchemaFieldNumber.class) != null) {
                  throw new RuntimeException(
                      String.format(
                          "@SchemaFieldNumber can only be used on getters in Java Beans. Found on"
                              + " setter '%s'",
                          t.getMethod().getName()));
                }
                if (t.getMethod().getAnnotation(SchemaFieldName.class) != null) {
                  throw new RuntimeException(
                      String.format(
                          "@SchemaFieldName can only be used on getters in Java Beans. Found on"
                              + " setter '%s'",
                          t.getMethod().getName()));
                }
                if (t.getMethod().getAnnotation(SchemaCaseFormat.class) != null) {
                  throw new RuntimeException(
                      String.format(
                          "@SchemaCaseFormat can only be used on getters in Java Beans. Found on"
                              + " setter '%s'",
                          t.getMethod().getName()));
                }
                return t;
              })
          .collect(Collectors.toList());
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj != null && this.getClass() == obj.getClass();
    }
  }

  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(typeDescriptor, GetterTypeSupplier.INSTANCE);

    // If there are no creator methods, then validate that we have setters for every field.
    // Otherwise, we will have no way of creating instances of the class.
    if (ReflectUtils.getAnnotatedCreateMethod(typeDescriptor.getRawType()) == null
        && ReflectUtils.getAnnotatedConstructor(typeDescriptor.getRawType()) == null) {
      JavaBeanUtils.validateJavaBean(
          GetterTypeSupplier.INSTANCE.get(typeDescriptor, schema),
          SetterTypeSupplier.INSTANCE.get(typeDescriptor, schema),
          schema);
    }
    return schema;
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return JavaBeanUtils.getGetters(
        targetTypeDescriptor,
        schema,
        GetterTypeSupplier.INSTANCE,
        new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
    return JavaBeanUtils.getFieldTypes(targetTypeDescriptor, schema, GetterTypeSupplier.INSTANCE);
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
          GetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    // If a Constructor was tagged with @SchemaCreate, invoke that constructor.
    Constructor<?> constructor =
        ReflectUtils.getAnnotatedConstructor(targetTypeDescriptor.getRawType());
    if (constructor != null) {
      return JavaBeanUtils.getConstructorCreator(
          targetTypeDescriptor,
          constructor,
          schema,
          GetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    // Else try to make a setter-based creator
    Factory<SchemaUserTypeCreator> setterBasedFactory =
        new SetterBasedCreatorFactory(new JavaBeanSetterFactory());
    return setterBasedFactory.create(targetTypeDescriptor, schema);
  }

  /** A factory for creating {@link FieldValueSetter} objects for a JavaBean object. */
  private static class JavaBeanSetterFactory implements Factory<List<FieldValueSetter>> {
    @Override
    public List<FieldValueSetter> create(TypeDescriptor<?> targetTypeDescriptor, Schema schema) {
      return JavaBeanUtils.getSetters(
          targetTypeDescriptor,
          schema,
          SetterTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj != null && this.getClass() == obj.getClass();
  }
}
