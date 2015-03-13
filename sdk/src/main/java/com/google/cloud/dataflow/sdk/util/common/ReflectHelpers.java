/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.common;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

import java.lang.reflect.Method;

/**
 * Utilities for working with with {@link Class Classes} and {@link Method Methods}.
 */
public class ReflectHelpers {

  private static final Joiner COMMA_SEPARATOR = Joiner.on(", ");

  /** A {@link Function} which turns a method into a simple method signature. */
  public static final Function<Method, String> METHOD_FORMATTER = new Function<Method, String>() {
    @Override
    public String apply(Method input) {
      String parameterTypes = FluentIterable.of(input.getParameterTypes())
          .transform(CLASS_SIMPLE_NAME)
          .join(COMMA_SEPARATOR);
      return String.format("%s(%s)",
          input.getName(),
          parameterTypes);
    }
  };

  /** A {@link Function} which turns a method into the declaring class + method signature. */
  public static final Function<Method, String> CLASS_AND_METHOD_FORMATTER =
      new Function<Method, String>() {
    @Override
    public String apply(Method input) {
      return String.format("%s#%s",
          CLASS_NAME.apply(input.getDeclaringClass()),
          METHOD_FORMATTER.apply(input));
    }
  };

  /** A {@link Function} with returns the classes name. */
  public static final Function<Class<?>, String> CLASS_NAME =
      new Function<Class<?>, String>(){
    @Override
    public String apply(Class<?> input) {
      return input.getName();
    }
  };

  /** A {@link Function} with returns the classes name. */
  public static final Function<Class<?>, String> CLASS_SIMPLE_NAME =
      new Function<Class<?>, String>(){
    @Override
    public String apply(Class<?> input) {
      return input.getSimpleName();
    }
  };
}
