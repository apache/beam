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

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.annotations.FieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.values.TypeDescriptor;

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
@Experimental(Kind.SCHEMAS)
public class JavaBeanSchema extends GetterBasedSchemaProvider {
  /** {@link FieldValueTypeSupplier} that's based on getter methods. */
  @VisibleForTesting
  public static class GetterTypeSupplier implements FieldValueTypeSupplier {
    public static final GetterTypeSupplier INSTANCE = new GetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz, @Nullable Schema schema) {
      List<FieldValueTypeInformation> types =
          ReflectUtils.getMethods(clazz)
              .stream()
              .filter(ReflectUtils::isGetter)
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

  /** {@link FieldValueTypeSupplier} that's based on setter methods. */
  @VisibleForTesting
  public static class SetterTypeSupplier implements FieldValueTypeSupplier {
    private static final SetterTypeSupplier INSTANCE = new SetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz, Schema schema) {
      List<FieldValueTypeInformation> types =
          ReflectUtils.getMethods(clazz)
              .stream()
              .filter(ReflectUtils::isSetter)
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .map(FieldValueTypeInformation::forSetter)
              .collect(Collectors.toList());

      return (schema != null) ? StaticSchemaInference.sortBySchema(types, schema) : types;
    }
  }

  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            typeDescriptor.getRawType(), GetterTypeSupplier.INSTANCE);
    return schema;
  }

  @Override
  public FieldValueGetterFactory fieldValueGetterFactory() {
    return (Class<?> targetClass, Schema schema) ->
        JavaBeanUtils.getGetters(targetClass, schema, GetterTypeSupplier.INSTANCE);
  }

  @Override
  UserTypeCreatorFactory schemaTypeCreatorFactory() {
    return (Class<?> targetClass, Schema schema) -> {
      // If a static method is marked with @SchemaCreate, use that.
      Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetClass);
      if (annotated != null) {
        return JavaBeanUtils.getStaticCreator(
            targetClass, annotated, schema, GetterTypeSupplier.INSTANCE);
      }

      // If a Constructor was tagged with @SchemaCreate, invoke that constructor.
      Constructor<?> constructor = ReflectUtils.getAnnotatedConstructor(targetClass);
      if (constructor != null) {
        return JavaBeanUtils.getConstructorCreator(
            targetClass, constructor, schema, GetterTypeSupplier.INSTANCE);
      }

      // If nothing else is available, then look for setters and generate a creator that uses them.
      JavaBeanUtils.validateJavaBean(
          GetterTypeSupplier.INSTANCE.get(targetClass, schema),
          SetterTypeSupplier.INSTANCE.get(targetClass, schema));
      return JavaBeanUtils.getSetFieldCreator(targetClass, schema, SetterTypeSupplier.INSTANCE);
    };
  }

  @Override
  public FieldValueTypeInformationFactory fieldValueTypeInformationFactory() {
    return (Class<?> targetClass, Schema schema) ->
        JavaBeanUtils.getFieldTypes(targetClass, schema, GetterTypeSupplier.INSTANCE);
  }
}
