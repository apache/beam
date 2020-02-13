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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.POJOUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A {@link SchemaProvider} for Java POJO objects.
 *
 * <p>This provider finds all public fields (recursively) in a Java object, and creates schemas and
 * rows that bind to those fields. The field order in the schema is not guaranteed to match the
 * field order in the class. The Java object is expected to have implemented a correct .equals() and
 * .hashCode() methods. The equals method must be completely determined by the schema fields. i.e.
 * if the object has hidden fields that are not reflected in the schema but are compared in equals,
 * then results will be incorrect.
 *
 * <p>TODO: Validate equals() method is provided, and if not generate a "slow" equals method based
 * on the schema.
 */
@Experimental(Kind.SCHEMAS)
public class JavaFieldSchema extends GetterBasedSchemaProvider {
  /** {@link FieldValueTypeSupplier} that's based on public fields. */
  @VisibleForTesting
  public static class JavaFieldTypeSupplier implements FieldValueTypeSupplier {
    public static final JavaFieldTypeSupplier INSTANCE = new JavaFieldTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      List<FieldValueTypeInformation> types =
          ReflectUtils.getFields(clazz).stream()
              .filter(f -> !f.isAnnotationPresent(SchemaIgnore.class))
              .map(FieldValueTypeInformation::forField)
              .map(
                  t -> {
                    SchemaFieldName fieldName = t.getField().getAnnotation(SchemaFieldName.class);
                    return (fieldName != null) ? t.withName(fieldName.value()) : t;
                  })
              .collect(Collectors.toList());

      // If there are no creators registered, then make sure none of the schema fields are final,
      // as we (currently) have no way of creating classes in this case.
      if (ReflectUtils.getAnnotatedCreateMethod(clazz) == null
          && ReflectUtils.getAnnotatedConstructor(clazz) == null) {
        Optional<Field> finalField =
            types.stream()
                .map(FieldValueTypeInformation::getField)
                .filter(f -> Modifier.isFinal(f.getModifiers()))
                .findAny();
        if (finalField.isPresent()) {
          throw new IllegalArgumentException(
              "Class "
                  + clazz
                  + " has final fields and no "
                  + "registered creator. Cannot use as schema, as we don't know how to create this "
                  + "object automatically");
        }
      }
      return types;
    }
  }

  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return POJOUtils.schemaFromPojoClass(
        typeDescriptor.getRawType(), JavaFieldTypeSupplier.INSTANCE);
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return POJOUtils.getGetters(
        targetClass, schema, JavaFieldTypeSupplier.INSTANCE, new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    return POJOUtils.getFieldTypes(targetClass, schema, JavaFieldTypeSupplier.INSTANCE);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    // If a static method is marked with @SchemaCreate, use that.
    Method annotated = ReflectUtils.getAnnotatedCreateMethod(targetClass);
    if (annotated != null) {
      return POJOUtils.getStaticCreator(
          targetClass,
          annotated,
          schema,
          JavaFieldTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    // If a Constructor was tagged with @SchemaCreate, invoke that constructor.
    Constructor<?> constructor = ReflectUtils.getAnnotatedConstructor(targetClass);
    if (constructor != null) {
      return POJOUtils.getConstructorCreator(
          targetClass,
          constructor,
          schema,
          JavaFieldTypeSupplier.INSTANCE,
          new DefaultTypeConversionsFactory());
    }

    return POJOUtils.getSetFieldCreator(
        targetClass, schema, JavaFieldTypeSupplier.INSTANCE, new DefaultTypeConversionsFactory());
  }
}
