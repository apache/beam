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
package org.apache.beam.examples.complete.cdap;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class for getting any filled {@link io.cdap.cdap.api.plugin.PluginConfig} configuration object.
 */
public class PluginConfigInstantiationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PluginConfigInstantiationUtils.class);

    /**
     * @param params map of config fields, where key is the name of the field, value must be String or boxed primitive
     * @return Config object for given map of arguments and configuration class
     */
    public static <T extends PluginConfig> T getPluginConfig(Map<String, Object> params, Class<T> configClass) {
        //Validate configClass
        if (configClass == null || configClass.isPrimitive()
                || configClass.isArray()) {
            throw new IllegalArgumentException("Config class must be correct!");
        }
        List<Field> allFields = new ArrayList<>();
        Class<?> currClass = configClass;
        while (!currClass.equals(Object.class)) {
            allFields.addAll(Arrays.stream(currClass.getDeclaredFields())
                    .filter(f -> !Modifier.isStatic(f.getModifiers())
                            && f.isAnnotationPresent(Name.class)).collect(Collectors.toList()));
            currClass = currClass.getSuperclass();
        }
        T config = getEmptyObjectOf(configClass);

        for (Field field : allFields) {
            field.setAccessible(true);

            Class<?> fieldType = field.getType();

            String fieldName = field.getDeclaredAnnotation(Name.class).value();
            Object fieldValue = params.get(fieldName);

            if (fieldValue != null && fieldType.equals(fieldValue.getClass())) {
                try {
                    field.set(config, fieldValue);
                } catch (IllegalAccessException e) {
                    LOG.error("Can not set a field", e);
                }
            }
        }
        return config;
    }

    /**
     * @return empty {@link Object} of {@param tClass}
     */
    private static <T> T getEmptyObjectOf(Class<T> tClass) {
        for (Constructor<?> constructor : tClass.getDeclaredConstructors()) {
            constructor.setAccessible(true);
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            Object[] parameters = Arrays.stream(parameterTypes)
                    .map(PluginConfigInstantiationUtils::getDefaultValue).toArray();
            try {
                return (T) constructor.newInstance(parameters);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                LOG.error("Can not instantiate an empty object", e);
            }
        }
        return null;
    }

    /**
     * @return default value for given {@param tClass}
     */
    private static Object getDefaultValue(Class<?> tClass) {
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
