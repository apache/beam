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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DefaultCoder} annotation specifies a {@link Coder} class to handle encoding and
 * decoding instances of the annotated class.
 *
 * <p>The specified {@link Coder} must have the following method:
 * <pre>
 * {@code public static CoderFactory getCoderFactory()}.
 * </pre>
 *
 * <p>Coders specified explicitly via {@link PCollection#setCoder} take precedence, followed by
 * Coders found at runtime via {@link CoderRegistry#getDefaultCoder}.
 * See {@link CoderRegistry} for a more detailed discussion of the precedence rules.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SuppressWarnings("rawtypes")
public @interface DefaultCoder {
  Class<? extends Coder> value();

  /**
   * A {@link CoderFactoryRegistrar} that registers a {@link CoderFactory} which can use
   * the {@code @DefaultCoder} annotation to provide {@link CoderFactory coder factories} that
   * creates {@link Coder}s.
   */
  @AutoService(CoderFactoryRegistrar.class)
  class DefaultCoderFactoryRegistrar implements CoderFactoryRegistrar {

    @Override
    public List<CoderFactory> getCoderFactories() {
      return ImmutableList.<CoderFactory>of(new DefaultCoderFactory());
    }

    /**
     * A {@link CoderFactory} that uses the {@code @DefaultCoder} annotation to provide
     * {@link CoderFactory coder factories} that create {@link Coder}s.
     */
    static class DefaultCoderFactory implements CoderFactory {
      private static final Logger LOG = LoggerFactory.getLogger(DefaultCoderFactory.class);

      /**
       * Returns the {@link Coder} returned according to the {@link CoderFactory} from any
       * {@link DefaultCoder} annotation on the given class.
       */
      @Override
      public <T> Coder<T> create(TypeDescriptor<T> typeDescriptor,
          List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {

        Class<?> clazz = typeDescriptor.getRawType();
        DefaultCoder defaultAnnotation = clazz.getAnnotation(DefaultCoder.class);
        if (defaultAnnotation == null) {
          throw new CannotProvideCoderException(
              String.format("Class %s does not have a @DefaultCoder annotation.",
                  clazz.getName()));
        }

        if (defaultAnnotation.value() == null) {
          throw new CannotProvideCoderException(
              String.format("Class %s has a @DefaultCoder annotation with a null value.",
                  clazz.getName()));
        }

        LOG.debug("DefaultCoder annotation found for {} with value {}",
            clazz, defaultAnnotation.value());

        Method coderFactoryMethod;
        try {
          coderFactoryMethod = defaultAnnotation.value().getMethod("getCoderFactory");
        } catch (NoSuchMethodException e) {
          throw new CannotProvideCoderException(String.format(
              "Unable to find 'public static CoderFactory getCoderFactory' on %s",
              defaultAnnotation.value()),
              e);
        }

        CoderFactory coderFactory;
        try {
          coderFactory = (CoderFactory) coderFactoryMethod.invoke(null);
        } catch (IllegalAccessException
            | IllegalArgumentException
            | InvocationTargetException
            | NullPointerException
            | ExceptionInInitializerError e) {
          throw new CannotProvideCoderException(String.format(
              "Unable to invoke 'public static CoderFactory getCoderFactory' on %s",
              defaultAnnotation.value()),
              e);
        }
        return coderFactory.create(typeDescriptor, componentCoders);
      }
    }
  }
}
