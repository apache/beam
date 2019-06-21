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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;

/** This is a utility class to set and remove options individually. */
public class PipelineOptionsReflectionSetter {
  private static final boolean STRICT_PARSING = true;

  @SuppressWarnings("unchecked")
  public static Class<? extends PipelineOptions> getPipelineOptionsInterface(
      PipelineOptions options) {
    if (options.getClass().getInterfaces().length != 1) {
      throw new IllegalArgumentException(
          "The pipeline option instance must implement exactly one interface");
    }
    return (Class<? extends PipelineOptions>) options.getClass().getInterfaces()[0];
  }

  private static PropertyDescriptor getProperty(Class optionsClass, String key) {
    Map<String, PropertyDescriptor> pmap = new HashMap<>();
    try {
      for (PropertyDescriptor pd :
          Introspector.getBeanInfo(optionsClass).getPropertyDescriptors()) {
        pmap.put(pd.getName(), pd);
      }
    } catch (IntrospectionException e) {
      throw new RuntimeException("Cannot get the properties of pipeline options", e);
    }

    if (!pmap.containsKey(key)) {
      SortedSet<String> closestMatches =
          new TreeSet<>(
              Sets.filter(
                  pmap.keySet(), input -> StringUtils.getLevenshteinDistance(key, input) <= 2));
      switch (closestMatches.size()) {
        case 0:
          throw new IllegalArgumentException(
              String.format("PipelineOptions missing a property named '%s'.", key));
        case 1:
          throw new IllegalArgumentException(
              String.format(
                  "PipelineOptions missing a property named '%s'. Did you mean '%s'?",
                  key, Iterables.getOnlyElement(closestMatches)));
        default:
          throw new IllegalArgumentException(
              String.format(
                  "PipelineOptions missing a property named '%s'. Did you mean one of %s?",
                  key, closestMatches));
      }
    }
    return pmap.get(key);
  }

  public static void setOption(PipelineOptions options, String key, String value) {
    Class<? extends PipelineOptions> interfaceClass = getPipelineOptionsInterface(options);
    PropertyDescriptor propertyDescriptor = getProperty(options.getClass(), key);
    ListMultimap<String, String> multimap = ImmutableListMultimap.of(key, value);

    Object valueObject =
        PipelineOptionsFactory.parseObjects(interfaceClass, multimap, STRICT_PARSING).get(key);

    try {
      propertyDescriptor.getWriteMethod().invoke(options, valueObject);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Cannot set property " + key, e);
    }
  }

  public static void removeOption(PipelineOptions options, String key) {
    Class<? extends PipelineOptions> interfaceClass = getPipelineOptionsInterface(options);
    PipelineOptions defaultOption = PipelineOptionsFactory.as(interfaceClass);
    PropertyDescriptor propertyDescriptor = getProperty(options.getClass(), key);
    Object defaultValue;
    try {
      defaultValue = propertyDescriptor.getReadMethod().invoke(defaultOption);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Cannot set property " + key, e);
    }
    try {
      propertyDescriptor.getWriteMethod().invoke(options, defaultValue);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Cannot set property " + key, e);
    }
  }
}
