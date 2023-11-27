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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

/** Static utility methods for creating and working with {@link CoderProvider}s. */
public final class CoderProviders {
  private CoderProviders() {} // Static utility class

  /**
   * Creates a {@link CoderProvider} from a class's {@code static <T> Coder<T> of(TypeDescriptor<T>,
   * List<Coder<?>>}) method.
   */
  public static CoderProvider fromStaticMethods(Class<?> rawType, Class<?> coderClazz) {
    checkArgument(
        Coder.class.isAssignableFrom(coderClazz),
        "%s is not a subtype of %s",
        coderClazz.getName(),
        Coder.class.getSimpleName());
    return new CoderProviderFromStaticMethods(rawType, coderClazz);
  }

  /** Creates a {@link CoderProvider} that always returns the given coder for the specified type. */
  public static CoderProvider forCoder(TypeDescriptor<?> type, Coder<?> coder) {
    return new CoderProviderForCoder(type, coder);
  }

  /**
   * See {@link #fromStaticMethods} for a detailed description of the characteristics of this {@link
   * CoderProvider}.
   */
  private static class CoderProviderFromStaticMethods extends CoderProvider {

    @Override
    public <T> Coder<T> coderFor(TypeDescriptor<T> type, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!this.rawType.equals(type.getRawType())) {
        throw new CannotProvideCoderException(
            String.format(
                "Unable to provide coder for %s, this factory can only provide coders for %s",
                type, this.rawType));
      }
      try {
        return (Coder<T>)
            Preconditions.checkStateNotNull(
                factoryMethod.invoke(this.rawType /* ignored */, componentCoders.toArray()));
      } catch (IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | NullPointerException
          | ExceptionInInitializerError exn) {
        throw new IllegalStateException(
            "error when invoking Coder factory method " + factoryMethod, exn);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////

    // Type raw type used to filter the incoming type on.
    private final Class<?> rawType;

    // Method to create a coder given component coders
    // For a Coder class of kind * -> * -> ... n times ... -> *
    // this has type Coder<?> -> Coder<?> -> ... n times ... -> Coder<T>
    private final Method factoryMethod;

    /** Returns a CoderProvider that invokes the given static factory method to create the Coder. */
    private CoderProviderFromStaticMethods(Class<?> rawType, Class<?> coderClazz) {
      this.rawType = rawType;
      this.factoryMethod = getFactoryMethod(coderClazz);
    }

    /**
     * Returns the static {@code of} constructor method on {@code coderClazz} if it exists. It is
     * assumed to have one {@link Coder} parameter for each type parameter of {@code coderClazz}.
     */
    private static Method getFactoryMethod(Class<?> coderClazz) {
      Method factoryMethodCandidate;

      // Find the static factory method of coderClazz named 'of' with
      // the appropriate number of type parameters.
      int numTypeParameters = coderClazz.getTypeParameters().length;
      Class<?>[] factoryMethodArgTypes = new Class<?>[numTypeParameters];
      Arrays.fill(factoryMethodArgTypes, Coder.class);
      try {
        factoryMethodCandidate = coderClazz.getDeclaredMethod("of", factoryMethodArgTypes);
      } catch (NoSuchMethodException | SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder "
                + coderClazz
                + ": "
                + "does not have an accessible method named 'of' with "
                + numTypeParameters
                + " arguments of Coder type",
            exn);
      }
      if (!Modifier.isStatic(factoryMethodCandidate.getModifiers())) {
        throw new IllegalArgumentException(
            "cannot register Coder "
                + coderClazz
                + ": "
                + "method named 'of' with "
                + numTypeParameters
                + " arguments of Coder type is not static");
      }
      if (!coderClazz.isAssignableFrom(factoryMethodCandidate.getReturnType())) {
        throw new IllegalArgumentException(
            "cannot register Coder "
                + coderClazz
                + ": "
                + "method named 'of' with "
                + numTypeParameters
                + " arguments of Coder type does not return a "
                + coderClazz);
      }
      try {
        if (!factoryMethodCandidate.isAccessible()) {
          factoryMethodCandidate.setAccessible(true);
        }
      } catch (SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder "
                + coderClazz
                + ": "
                + "method named 'of' with "
                + numTypeParameters
                + " arguments of Coder type is not accessible",
            exn);
      }

      return factoryMethodCandidate;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("rawType", rawType)
          .add("factoryMethod", factoryMethod)
          .toString();
    }
  }

  /** See {@link #forCoder} for a detailed description of this {@link CoderProvider}. */
  private static class CoderProviderForCoder extends CoderProvider {
    private final Coder<?> coder;
    private final TypeDescriptor<?> type;

    public CoderProviderForCoder(TypeDescriptor<?> type, Coder<?> coder) {
      this.type = type;
      this.coder = coder;
    }

    @Override
    public <T> Coder<T> coderFor(TypeDescriptor<T> type, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      boolean isTypeEqual = this.type.equals(type);
      boolean isAutoValueConcrete =
          type.getRawType().getName().contains("AutoValue_")
              && this.type.getRawType().isAssignableFrom(type.getRawType());

      if (!isTypeEqual && !isAutoValueConcrete) {
        throw new CannotProvideCoderException(
            String.format(
                "Unable to provide coder for %s, this factory can only provide coders for %s",
                type, this.type));
      }
      return (Coder) coder;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("type", type)
          .add("coder", coder)
          .toString();
    }
  }
}
