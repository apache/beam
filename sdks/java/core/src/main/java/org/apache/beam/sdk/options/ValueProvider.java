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
package org.apache.beam.sdk.options;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** {@link ValueProvider} is an interface which abstracts the notion of
 * fetching a value that may or may not be currently available.  This can be
 * used to parameterize transforms that only read values in at runtime, for
 * example.
 */
public interface ValueProvider<T> {
  T get();

  /** Whether the contents of this ValueProvider is available to validation
   * routines that run at graph construction time.
   */
  boolean shouldValidate();

  /** {@link StaticValueProvider} is an implementation of ValueProvider that
   * allows for a static value to be provided.
   */
  public static class StaticValueProvider<T> implements ValueProvider<T>, Serializable {
    private final T value;

    StaticValueProvider(T value) {
      this.value = value;
    }

    public static <T> StaticValueProvider<T> of(T value) {
      StaticValueProvider<T> factory = new StaticValueProvider<>(value);
      return factory;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public boolean shouldValidate() {
      return true;
    }
  }

  /** {@link RuntimeValueProvider} is an implementation of ValueProvider that
   * allows for a value to be provided at execution time rather than at graph
   * construction time.
   *
   * <p>To enforce this contract, if there is no default, users must only call
   * get() on the worker, which will provide the value of OPTIONS.
   */
  public static class RuntimeValueProvider<T> implements ValueProvider<T>, Serializable {
    private static PipelineOptions options = null;

    private final Class<? extends PipelineOptions> klass;
    private final String methodName;
    private final T defaultValue;

    RuntimeValueProvider(String methodName, Class<? extends PipelineOptions> klass) {
      this.methodName = methodName;
      this.klass = klass;
      this.defaultValue = null;
    }

    RuntimeValueProvider(String methodName, Class<? extends PipelineOptions> klass,
      T defaultValue) {
      this.methodName = methodName;
      this.klass = klass;
      this.defaultValue = defaultValue;
    }

    static void setRuntimeOptions(PipelineOptions runtimeOptions) {
      options = runtimeOptions;
    }

    @Override
    public T get() {
      if (options == null) {
        if (defaultValue != null) {
          return defaultValue;
        }
        throw new RuntimeException("Not called from a runtime context.");
      }
      try {
        Method method = klass.getMethod(methodName);
        PipelineOptions methodOptions = options.as(klass);
        InvocationHandler handler = Proxy.getInvocationHandler(methodOptions);
        return (T) handler.invoke(methodOptions, method, null);
      } catch (Throwable e) {
        throw new RuntimeException("Unable to load runtime value.", e);
      }
    }

    @Override
    public boolean shouldValidate() {
      return defaultValue != null;
    }
  }
}
