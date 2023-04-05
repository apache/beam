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

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;

/** Utilities to reflect over {@link PipelineOptions}. */
class PipelineOptionsReflector {
  private PipelineOptionsReflector() {}

  /**
   * Retrieve metadata for the full set of pipeline options within the type hierarchy of a single
   * {@link PipelineOptions} interface with optional filtering {@link Hidden} options.
   *
   * @see PipelineOptionsReflector#getOptionSpecs(Iterable)
   */
  static Set<PipelineOptionSpec> getOptionSpecs(
      Class<? extends PipelineOptions> optionsInterface, boolean skipHidden) {
    Iterable<Method> methods = ReflectHelpers.getClosureOfMethodsOnInterface(optionsInterface);
    Multimap<String, Method> propsToGetters = getPropertyNamesToGetters(methods);

    ImmutableSet.Builder<PipelineOptionSpec> setBuilder = ImmutableSet.builder();
    for (Map.Entry<String, Method> propAndGetter : propsToGetters.entries()) {
      String prop = propAndGetter.getKey();
      Method getter = propAndGetter.getValue();

      @SuppressWarnings("unchecked")
      Class<? extends PipelineOptions> declaringClass =
          (Class<? extends PipelineOptions>) getter.getDeclaringClass();

      if (!PipelineOptions.class.isAssignableFrom(declaringClass)) {
        continue;
      }

      if (skipHidden && declaringClass.isAnnotationPresent(Hidden.class)) {
        continue;
      }

      setBuilder.add(PipelineOptionSpec.of(declaringClass, prop, getter));
    }

    return setBuilder.build();
  }

  /**
   * Retrieve metadata for the full set of pipeline options visible within the type hierarchy
   * closure of the set of input interfaces. An option is "visible" if:
   *
   * <ul>
   *   <li>The option is defined within the interface hierarchy closure of the input {@link
   *       PipelineOptions}.
   *   <li>The defining interface is not marked {@link Hidden}.
   * </ul>
   */
  static Set<PipelineOptionSpec> getOptionSpecs(
      Iterable<Class<? extends PipelineOptions>> optionsInterfaces) {
    ImmutableSet.Builder<PipelineOptionSpec> setBuilder = ImmutableSet.builder();
    for (Class<? extends PipelineOptions> optionsInterface : optionsInterfaces) {
      setBuilder.addAll(getOptionSpecs(optionsInterface, true));
    }

    return setBuilder.build();
  }

  /**
   * Extract pipeline options and their respective getter methods from a series of {@link Method
   * methods}. A single pipeline option may appear in many methods.
   *
   * @return A mapping of option name to the input methods which declare it.
   */
  static Multimap<String, Method> getPropertyNamesToGetters(Iterable<Method> methods) {
    Multimap<String, Method> propertyNamesToGetters = HashMultimap.create();
    for (Method method : methods) {
      String methodName = method.getName();
      if ((!methodName.startsWith("get") && !methodName.startsWith("is"))
          || method.getParameterTypes().length != 0
          || method.getReturnType() == void.class) {
        continue;
      }
      String propertyName =
          Introspector.decapitalize(
              methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3));
      propertyNamesToGetters.put(propertyName, method);
    }
    return propertyNamesToGetters;
  }
}
