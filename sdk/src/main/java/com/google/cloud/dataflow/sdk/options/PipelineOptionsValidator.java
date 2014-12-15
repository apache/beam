/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.options;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Validates that the {@link PipelineOptions} conforms to all the {@link Validation} criteria.
 */
public class PipelineOptionsValidator {
  /**
   * Validates that the passed {@link PipelineOptions} conforms to all the validation criteria from
   * the passed in interface.
   * <p>
   * Note that the interface requested must conform to the validation criteria specified on
   * {@link PipelineOptions#as(Class)}.
   *
   * @param klass The interface to fetch validation criteria from.
   * @param options The {@link PipelineOptions} to validate.
   * @return The type
   */
  public static <T extends PipelineOptions> T validate(Class<T> klass, PipelineOptions options) {
    Preconditions.checkNotNull(klass);
    Preconditions.checkNotNull(options);
    Preconditions.checkArgument(Proxy.isProxyClass(options.getClass()));
    Preconditions.checkArgument(Proxy.getInvocationHandler(options)
        instanceof ProxyInvocationHandler);

    ProxyInvocationHandler handler =
        (ProxyInvocationHandler) Proxy.getInvocationHandler(options);
    for (Method method : PipelineOptionsFactory.getClosureOfMethodsOnInterface(klass)) {
      if (method.getAnnotation(Validation.Required.class) != null) {
        Preconditions.checkArgument(handler.invoke(options, method, null) != null,
            "Missing required value for [" + method + ", \"" + getDescription(method) + "\"]. ");
      }
    }
    return options.as(klass);
  }

  private static String getDescription(Method method) {
    Description description = method.getAnnotation(Description.class);
    return description == null ? "" : description.value();
  }
}

