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
package org.apache.beam.sdk.io.sparkreceiver;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for getting any filled {@link PluginConfig} configuration object. */
@SuppressWarnings({"unchecked", "assignment.type.incompatible"})
public class PluginConfigInstantiationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PluginConfigInstantiationUtils.class);

  /**
   * Method for instantiating {@link PluginConfig} object of specific class {@param configClass}.
   * After instantiating, it will go over all {@link Field}s with the {@link Name} annotation and
   * set the appropriate parameter values from the {@param params} map for them.
   *
   * @param params map of config fields, where key is the name of the field, value must be String or
   *     boxed primitive
   * @return Config object for given map of arguments and configuration class
   */
  static @Nullable <T extends PluginConfig> T getPluginConfig(
      Map<String, Object> params, Class<T> configClass) {
    // Validate configClass
    if (configClass == null) {
      throw new IllegalArgumentException("Config class must be not null!");
    }
    List<Field> allFields = new ArrayList<>();
    Class<?> currClass = configClass;
    while (currClass != null && !currClass.equals(Object.class)) {
      allFields.addAll(
          Arrays.stream(currClass.getDeclaredFields())
              .filter(
                  f -> !Modifier.isStatic(f.getModifiers()) && f.isAnnotationPresent(Name.class))
              .collect(Collectors.toList()));
      currClass = currClass.getSuperclass();
    }
    T config = getEmptyObjectFromDefaultValues(configClass);

    if (config != null) {
      for (Field field : allFields) {
        field.setAccessible(true);

        Class<?> fieldType = field.getType();

        Name declaredAnnotation = field.getDeclaredAnnotation(Name.class);
        Object fieldValue =
            declaredAnnotation != null ? params.get(declaredAnnotation.value()) : null;

        if (fieldValue != null && fieldType.equals(fieldValue.getClass())) {
          try {
            field.set(config, fieldValue);
          } catch (IllegalAccessException e) {
            LOG.error("Can not set a field with value {}", fieldValue);
          }
        }
      }
    }
    return config;
  }

  /** @return empty {@link Object} of {@param tClass} */
  private static @Nullable <T> T getEmptyObjectFromDefaultValues(Class<T> tClass) {
    for (Constructor<?> constructor : tClass.getDeclaredConstructors()) {
      constructor.setAccessible(true);
      Class<?>[] parameterTypes = constructor.getParameterTypes();
      Object[] parameters = new Object[parameterTypes.length];
      for (int i = 0; i < parameterTypes.length; i++) {
        parameters[i] = getDefaultValue(parameterTypes[i]);
      }
      try {
        return (T) constructor.newInstance(parameters);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        LOG.warn("Can not instantiate an empty object", e);
      }
    }
    return null;
  }

  /** @return default value for given {@param tClass} if it's primitive, otherwise returns null */
  private static @Nullable Object getDefaultValue(@Nullable Class<?> tClass) {
    if (Boolean.TYPE.equals(tClass)) {
      return false;
    }
    if (Character.TYPE.equals(tClass)) {
      return Character.MIN_VALUE;
    }
    if (Byte.TYPE.equals(tClass)) {
      return Byte.MIN_VALUE;
    }
    if (Short.TYPE.equals(tClass)) {
      return Short.MIN_VALUE;
    }
    if (Double.TYPE.equals(tClass)) {
      return Double.MIN_VALUE;
    }
    if (Integer.TYPE.equals(tClass)) {
      return Integer.MIN_VALUE;
    }
    if (Float.TYPE.equals(tClass)) {
      return Float.MIN_VALUE;
    }
    if (Long.TYPE.equals(tClass)) {
      return Long.MIN_VALUE;
    }
    return null;
  }
}
