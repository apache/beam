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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Static utility methods for working with {@link CoderProvider CoderProviders}.
 */
public final class CoderProviders {

  // Static utility class
  private CoderProviders() { }

  /**
   * Creates a {@link CoderProvider} built from particular static methods of a class that
   * implements {@link Coder}. The requirements for this method are precisely the requirements
   * for a {@link Coder} class to be usable with {@link DefaultCoder} annotations.
   *
   * <p>The class must have the following static method:
   *
   * <pre>{@code
   * public static Coder<T> of(TypeDescriptor<T> type)
   * }
   * </pre>
   */
  public static <T> CoderProvider fromStaticMethods(Class<T> clazz) {
    return new CoderProviderFromStaticMethods(clazz);
  }


  /**
   * Returns a {@link CoderProvider} that consults each of the provider {@code coderProviders}
   * and returns the first {@link Coder} provided.
   *
   * <p>Note that the order in which the providers are listed matters: While the set of types
   * handled will be the union of those handled by all of the providers in the list, the actual
   * {@link Coder} provided by the first successful provider may differ, and may have inferior
   * properties. For example, not all {@link Coder Coders} are deterministic, handle {@code null}
   * values, or have comparable performance.
   */
  public static CoderProvider firstOf(CoderProvider... coderProviders) {
    return new FirstOf(ImmutableList.copyOf(coderProviders));
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * @see #firstOf
   */
  private static class FirstOf implements CoderProvider {

    private Iterable<CoderProvider> providers;

    public FirstOf(Iterable<CoderProvider> providers) {
      this.providers = providers;
    }

    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
      List<String> messages = Lists.newArrayList();
      for (CoderProvider provider : providers) {
        try {
          return provider.getCoder(type);
        } catch (CannotProvideCoderException exc) {
          messages.add(String.format("%s could not provide a Coder for type %s: %s",
              provider, type, exc.getMessage()));
        }
      }
      throw new CannotProvideCoderException(
          String.format("Cannot provide coder for type %s: %s.",
              type, Joiner.on("; ").join(messages)));
    }
  }

  private static class CoderProviderFromStaticMethods implements CoderProvider {

    /** If true, then clazz has {@code of(TypeDescriptor)}. If false, {@code of(Class)}. */
    private final boolean takesTypeDescriptor;
    private final Class<?> clazz;

    public CoderProviderFromStaticMethods(Class<?> clazz) {
      // Note that the second condition supports older classes, which only needed to provide
      // of(Class), not of(TypeDescriptor). Our own classes have updated to accept a
      // TypeDescriptor. Hence the error message points only to the current specification,
      // not both acceptable conditions.
      checkArgument(classTakesTypeDescriptor(clazz) || classTakesClass(clazz),
          "Class " + clazz.getCanonicalName()
          + " is missing required static method of(TypeDescriptor).");

      this.takesTypeDescriptor = classTakesTypeDescriptor(clazz);
      this.clazz = clazz;
    }

    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
      try {
        if (takesTypeDescriptor) {
          @SuppressWarnings("unchecked")
          Coder<T> result = InstanceBuilder.ofType(Coder.class)
              .fromClass(clazz)
              .fromFactoryMethod("of")
              .withArg(TypeDescriptor.class, type)
              .build();
          return result;
        } else {
          @SuppressWarnings("unchecked")
          Coder<T> result = InstanceBuilder.ofType(Coder.class)
              .fromClass(clazz)
              .fromFactoryMethod("of")
              .withArg(Class.class, type.getRawType())
              .build();
          return result;
        }
      } catch (RuntimeException exc) {
        if (exc.getCause() instanceof InvocationTargetException) {
          throw new CannotProvideCoderException(exc.getCause().getCause());
        }
        throw exc;
      }
    }

    private boolean classTakesTypeDescriptor(Class<?> clazz) {
      try {
        clazz.getDeclaredMethod("of", TypeDescriptor.class);
        return true;
      } catch (NoSuchMethodException | SecurityException exc) {
        return false;
      }
    }

    private boolean classTakesClass(Class<?> clazz) {
      try {
        clazz.getDeclaredMethod("of", Class.class);
        return true;
      } catch (NoSuchMethodException | SecurityException exc) {
        return false;
      }
    }
  }
}
