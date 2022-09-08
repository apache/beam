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
package org.apache.beam.sdk.io.cdap;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for getting any filled {@link PluginConfig} configuration object. */
public class PluginConfigInstantiationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PluginConfigInstantiationUtils.class);
  private static final String MACRO_FIELDS_FIELD_NAME = "macroFields";

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
              .filter(f -> !Modifier.isStatic(f.getModifiers()))
              .collect(Collectors.toList()));
      currClass = currClass.getSuperclass();
    }
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    @Initialized T config = instantiatorFactory.get(TypeToken.of(configClass)).create();

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
        } else if (field.getName().equals(MACRO_FIELDS_FIELD_NAME)) {
          try {
            field.set(config, Collections.emptySet());
          } catch (IllegalAccessException e) {
            LOG.error("Can not set macro fields");
          }
        }
      }
    }
    return config;
  }
}
