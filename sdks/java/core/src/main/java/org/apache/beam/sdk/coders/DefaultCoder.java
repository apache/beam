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
package org.apache.beam.sdk.coders;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.CheckForNull;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DefaultCoder} annotation specifies a {@link Coder} class to handle encoding and
 * decoding instances of the annotated class.
 *
 * <p>The specified {@link Coder} must have the following method:
 *
 * <pre>
 * {@code public static CoderProvider getCoderProvider()}.
 * </pre>
 *
 * <p>Coders specified explicitly via {@link PCollection#setCoder} take precedence, followed by
 * Coders found at runtime via {@link CoderRegistry#getCoder}. See {@link CoderRegistry} for a more
 * detailed discussion of the precedence rules.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
})
public @interface DefaultCoder {
  @CheckForNull
  @SuppressWarnings("rawtypes") // this is deliberate, as the user will pass FooCoder.class
  Class<? extends Coder> value();

  /**
   * A {@link CoderProviderRegistrar} that registers a {@link CoderProvider} which can use the
   * {@code @DefaultCoder} annotation to provide {@link CoderProvider coder providers} that creates
   * {@link Coder}s.
   */
  class DefaultCoderProviderRegistrar implements CoderProviderRegistrar {

    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(new DefaultCoderProvider());
    }

    /**
     * A {@link CoderProvider} that uses the {@code @DefaultCoder} annotation to provide {@link
     * CoderProvider coder providers} that create {@link Coder}s.
     */
    public static class DefaultCoderProvider extends CoderProvider {
      private static final Logger LOG = LoggerFactory.getLogger(DefaultCoderProvider.class);

      /**
       * Returns the {@link Coder} returned according to the {@link CoderProvider} from any {@link
       * DefaultCoder} annotation on the given class.
       */
      @Override
      public <T> Coder<T> coderFor(
          TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
          throws CannotProvideCoderException {

        Class<?> clazz = typeDescriptor.getRawType();
        DefaultCoder defaultAnnotation = clazz.getAnnotation(DefaultCoder.class);
        if (defaultAnnotation == null) {
          // check if the superclass has DefaultCoder annotation if the class is generated using
          // AutoValue
          if (clazz.getName().contains("AutoValue_")) {
            clazz = clazz.getSuperclass();
            defaultAnnotation = clazz.getAnnotation(DefaultCoder.class);
          }
        }
        if (defaultAnnotation == null) {
          throw new CannotProvideCoderException(
              String.format("Class %s does not have a @DefaultCoder annotation.", clazz.getName()));
        }

        @SuppressWarnings("rawtypes") // this is deliberate, as the user will pass FooCoder.class
        Class<? extends Coder> defaultAnnotationValue = defaultAnnotation.value();
        if (defaultAnnotationValue == null) {
          throw new CannotProvideCoderException(
              String.format(
                  "Class %s has a @DefaultCoder annotation with a null value.", clazz.getName()));
        }

        LOG.debug(
            "DefaultCoder annotation found for {} with value {}", clazz, defaultAnnotationValue);

        Method coderProviderMethod;
        try {
          coderProviderMethod = defaultAnnotationValue.getMethod("getCoderProvider");
        } catch (NoSuchMethodException e) {
          throw new CannotProvideCoderException(
              String.format(
                  "Unable to find 'public static CoderProvider getCoderProvider()' on %s",
                  defaultAnnotationValue),
              e);
        }

        CoderProvider coderProvider;
        try {
          coderProvider =
              Preconditions.checkStateNotNull(
                  (CoderProvider) coderProviderMethod.invoke(clazz /* ignored */));
        } catch (IllegalAccessException
            | IllegalArgumentException
            | InvocationTargetException
            | NullPointerException
            | ExceptionInInitializerError e) {
          throw new CannotProvideCoderException(
              String.format(
                  "Unable to invoke 'public static CoderProvider getCoderProvider()' on %s",
                  defaultAnnotationValue),
              e);
        }
        return coderProvider.coderFor(typeDescriptor, componentCoders);
      }
    }
  }
}
