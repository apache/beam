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

/** Provides a value. */
public interface ValueProvider<T> {
  T get();

  boolean shouldValidate();

  /** This is for static values. */
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

  /** For dynamic values, must only call get() on the worker. */
  public static class RuntimeValueProvider<T> implements ValueProvider<T>, Serializable {
    // TODO(sgmc): Turn this into a keyed registry.
    public static PipelineOptions OPTIONS = null;

    private final Class<? extends PipelineOptions> klass;
    private final String methodName;

    RuntimeValueProvider(String methodName, Class<? extends PipelineOptions> klass) {
      this.methodName = methodName;
      this.klass = klass;
    }

    @Override
    public T get() {
      if (OPTIONS == null) {
        throw new RuntimeException("Not called from a worker context.");
      }
      try {
        Method method = klass.getMethod(methodName);
        PipelineOptions methodOptions = OPTIONS.as(klass);
        InvocationHandler handler = Proxy.getInvocationHandler(methodOptions);
        return (T) handler.invoke(methodOptions, method, null);
      } catch (Throwable e) {
        throw new RuntimeException("Unable to load runtime value.", e);
      }
    }

    @Override
    public boolean shouldValidate() {
      return false;
    }
  }
}
