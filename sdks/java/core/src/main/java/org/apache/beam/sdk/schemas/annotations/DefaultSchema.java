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
package org.apache.beam.sdk.schemas.annotations;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaProviderRegistrar;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The {@link DefaultSchema} annotation specifies a {@link SchemaProvider} class to handle obtaining
 * a schema and row for the specified class.
 *
 * <p>For example, if your class is JavaBean, the JavaBeanSchema provider class knows how to vend
 * schemas for this class. You can annotate it as follows:
 *
 * <pre>{@code @DefaultSchema(JavaBeanSchema.class)
 * class MyClass {
 *   public String getFoo();
 *   void setFoo(String foo);
 *         ....
 * }
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public @interface DefaultSchema {

  /** The schema provider implementation that knows how to vend schemas for the annotated class. */
  @CheckForNull
  Class<? extends SchemaProvider> value();

  /**
   * {@link SchemaProvider} for default schemas. Looks up the provider annotated for a type, and
   * delegates to that provider.
   */
  class DefaultSchemaProvider implements SchemaProvider {
    final Map<TypeDescriptor, ProviderAndDescriptor> cachedProviders = Maps.newConcurrentMap();

    private static final class ProviderAndDescriptor implements Serializable {
      final SchemaProvider schemaProvider;
      final TypeDescriptor<?> typeDescriptor;

      public ProviderAndDescriptor(
          SchemaProvider schemaProvider, TypeDescriptor<?> typeDescriptor) {
        this.schemaProvider = schemaProvider;
        this.typeDescriptor = typeDescriptor;
      }
    }

    private @Nullable ProviderAndDescriptor getSchemaProvider(TypeDescriptor<?> typeDescriptor) {
      return cachedProviders.computeIfAbsent(
          typeDescriptor,
          type -> {
            Class<?> clazz = type.getRawType();
            do {
              DefaultSchema annotation = clazz.getAnnotation(DefaultSchema.class);
              if (annotation != null) {
                Class<? extends SchemaProvider> providerClass = annotation.value();
                checkArgument(
                    providerClass != null,
                    "Type " + type + " has a @DefaultSchema annotation with a null argument.");

                try {
                  return new ProviderAndDescriptor(
                      providerClass.getDeclaredConstructor().newInstance(),
                      typeDescriptor.getSupertype((Class) clazz));
                } catch (NoSuchMethodException
                    | InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                  throw new IllegalStateException(
                      "Failed to create SchemaProvider "
                          + providerClass.getSimpleName()
                          + " which was"
                          + " specified as the default SchemaProvider for type "
                          + type
                          + ". Make "
                          + " sure that this class has a public default constructor.",
                      e);
                }
              }
              clazz = clazz.getSuperclass();
            } while (clazz != null && !clazz.equals(Object.class));
            return null;
          });
    }

    /**
     * Retrieves the underlying {@link SchemaProvider} for the given {@link TypeDescriptor}. If no
     * provider is found, returns null.
     */
    public @Nullable <T> SchemaProvider getUnderlyingSchemaProvider(
        TypeDescriptor<T> typeDescriptor) {
      ProviderAndDescriptor providerAndDescriptor = getSchemaProvider(typeDescriptor);
      return providerAndDescriptor != null ? providerAndDescriptor.schemaProvider : null;
    }

    /**
     * Retrieves the underlying {@link SchemaProvider} for the given {@link Class}. If no provider
     * is found, returns null.
     */
    public @Nullable <T> SchemaProvider getUnderlyingSchemaProvider(Class<T> clazz) {
      return getUnderlyingSchemaProvider(TypeDescriptor.of(clazz));
    }

    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      ProviderAndDescriptor providerAndDescriptor = getSchemaProvider(typeDescriptor);
      return (providerAndDescriptor != null)
          ? providerAndDescriptor.schemaProvider.schemaFor(providerAndDescriptor.typeDescriptor)
          : null;
    }

    /**
     * Given a type, return a function that converts that type to a {@link Row} object If no schema
     * exists, returns null.
     */
    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
      ProviderAndDescriptor providerAndDescriptor = getSchemaProvider(typeDescriptor);
      return (providerAndDescriptor != null)
          ? providerAndDescriptor.schemaProvider.toRowFunction(
              (TypeDescriptor<T>) providerAndDescriptor.typeDescriptor)
          : null;
    }

    /**
     * Given a type, returns a function that converts from a {@link Row} object to that type. If no
     * schema exists, returns null.
     */
    @Override
    public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
      ProviderAndDescriptor providerAndDescriptor = getSchemaProvider(typeDescriptor);
      return (providerAndDescriptor != null)
          ? providerAndDescriptor.schemaProvider.fromRowFunction(
              (TypeDescriptor<T>) providerAndDescriptor.typeDescriptor)
          : null;
    }
  }

  /** Registrar for default schemas. */
  class DefaultSchemaProviderRegistrar implements SchemaProviderRegistrar {
    @Override
    public List<SchemaProvider> getSchemaProviders() {
      return ImmutableList.of(new DefaultSchemaProvider());
    }
  }
}
