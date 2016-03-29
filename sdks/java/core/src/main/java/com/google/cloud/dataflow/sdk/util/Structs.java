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

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A collection of static methods for manipulating datastructure representations
 * transferred via the Dataflow API.
 */
public final class Structs {
  private Structs() {}  // Non-instantiable

  public static String getString(Map<String, Object> map, String name) throws Exception {
    return getValue(map, name, String.class, "a string");
  }

  public static String getString(
      Map<String, Object> map, String name, @Nullable String defaultValue)
      throws Exception {
    return getValue(map, name, String.class, "a string", defaultValue);
  }

  public static byte[] getBytes(Map<String, Object> map, String name) throws Exception {
    @Nullable byte[] result = getBytes(map, name, null);
    if (result == null) {
      throw new ParameterNotFoundException(name, map);
    }
    return result;
  }

  @Nullable
  public static byte[] getBytes(Map<String, Object> map, String name, @Nullable byte[] defaultValue)
      throws Exception {
    @Nullable String jsonString = getString(map, name, null);
    if (jsonString == null) {
      return defaultValue;
    }
    // TODO: Need to agree on a format for encoding bytes in
    // a string that can be sent to the backend, over the cloud
    // map task work API.  base64 encoding seems pretty common.  Switch to it?
    return StringUtils.jsonStringToByteArray(jsonString);
  }

  public static Boolean getBoolean(Map<String, Object> map, String name) throws Exception {
    return getValue(map, name, Boolean.class, "a boolean");
  }

  @Nullable
  public static Boolean getBoolean(
      Map<String, Object> map, String name, @Nullable Boolean defaultValue)
      throws Exception {
    return getValue(map, name, Boolean.class, "a boolean", defaultValue);
  }

  public static Long getLong(Map<String, Object> map, String name) throws Exception {
    return getValue(map, name, Long.class, "a long");
  }

  @Nullable
  public static Long getLong(Map<String, Object> map, String name, @Nullable Long defaultValue)
      throws Exception {
    return getValue(map, name, Long.class, "a long", defaultValue);
  }

  public static Integer getInt(Map<String, Object> map, String name) throws Exception {
    return getValue(map, name, Integer.class, "an int");
  }

  @Nullable
  public static Integer getInt(Map<String, Object> map, String name, @Nullable Integer defaultValue)
      throws Exception {
    return getValue(map, name, Integer.class, "an int", defaultValue);
  }

  @Nullable
  public static List<String> getStrings(
      Map<String, Object> map, String name, @Nullable List<String> defaultValue)
      throws Exception {
    @Nullable Object value = map.get(name);
    if (value == null) {
      if (map.containsKey(name)) {
        throw new IncorrectTypeException(name, map, "a string or a list");
      }
      return defaultValue;
    }
    if (Data.isNull(value)) {
      // This is a JSON literal null.  When represented as a list of strings,
      // this is an empty list.
      return Collections.<String>emptyList();
    }
    @Nullable String singletonString = decodeValue(value, String.class);
    if (singletonString != null) {
      return Collections.singletonList(singletonString);
    }
    if (!(value instanceof List)) {
      throw new IncorrectTypeException(name, map, "a string or a list");
    }
    @SuppressWarnings("unchecked")
    List<Object> elements = (List<Object>) value;
    List<String> result = new ArrayList<>(elements.size());
    for (Object o : elements) {
      @Nullable String s = decodeValue(o, String.class);
      if (s == null) {
        throw new IncorrectTypeException(name, map, "a list of strings");
      }
      result.add(s);
    }
    return result;
  }

  public static Map<String, Object> getObject(Map<String, Object> map, String name)
      throws Exception {
    @Nullable Map<String, Object> result = getObject(map, name, null);
    if (result == null) {
      throw new ParameterNotFoundException(name, map);
    }
    return result;
  }

  @Nullable
  public static Map<String, Object> getObject(
      Map<String, Object> map, String name, @Nullable Map<String, Object> defaultValue)
      throws Exception {
    @Nullable Object value = map.get(name);
    if (value == null) {
      if (map.containsKey(name)) {
        throw new IncorrectTypeException(name, map, "an object");
      }
      return defaultValue;
    }
    return checkObject(value, map, name);
  }

  private static Map<String, Object> checkObject(
      Object value, Map<String, Object> map, String name) throws Exception {
    if (Data.isNull(value)) {
      // This is a JSON literal null.  When represented as an object, this is an
      // empty map.
      return Collections.<String, Object>emptyMap();
    }
    if (!(value instanceof Map)) {
      throw new IncorrectTypeException(name, map, "an object (not a map)");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> mapValue = (Map<String, Object>) value;
    if (!mapValue.containsKey(PropertyNames.OBJECT_TYPE_NAME)) {
      throw new IncorrectTypeException(name, map,
          "an object (no \"" + PropertyNames.OBJECT_TYPE_NAME + "\" field)");
    }
    return mapValue;
  }

  @Nullable
  public static List<Map<String, Object>> getListOfMaps(Map<String, Object> map, String name,
      @Nullable List<Map<String, Object>> defaultValue) throws Exception {
    @Nullable
    Object value = map.get(name);
    if (value == null) {
      if (map.containsKey(name)) {
        throw new IncorrectTypeException(name, map, "a list");
      }
      return defaultValue;
    }
    if (Data.isNull(value)) {
      // This is a JSON literal null.  When represented as a list,
      // this is an empty list.
      return Collections.<Map<String, Object>>emptyList();
    }

    if (!(value instanceof List)) {
      throw new IncorrectTypeException(name, map, "a list");
    }

    List<?> elements = (List<?>) value;
    for (Object elem : elements) {
      if (!(elem instanceof Map)) {
        throw new IncorrectTypeException(name, map, "a list of Map objects");
      }
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result = (List<Map<String, Object>>) elements;
    return result;
  }

  public static Map<String, Object> getDictionary(
      Map<String, Object> map, String name) throws Exception {
    @Nullable Object value = map.get(name);
    if (value == null) {
      throw new ParameterNotFoundException(name, map);
    }
    if (Data.isNull(value)) {
      // This is a JSON literal null.  When represented as a dictionary, this is
      // an empty map.
      return Collections.<String, Object>emptyMap();
    }
    if (!(value instanceof Map)) {
      throw new IncorrectTypeException(name, map, "a dictionary");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) value;
    return result;
  }

  @Nullable
  public static Map<String, Object> getDictionary(
      Map<String, Object> map, String name, @Nullable Map<String, Object> defaultValue)
      throws Exception {
    @Nullable Object value = map.get(name);
    if (value == null) {
      if (map.containsKey(name)) {
        throw new IncorrectTypeException(name, map, "a dictionary");
      }
      return defaultValue;
    }
    if (Data.isNull(value)) {
      // This is a JSON literal null.  When represented as a dictionary, this is
      // an empty map.
      return Collections.<String, Object>emptyMap();
    }
    if (!(value instanceof Map)) {
      throw new IncorrectTypeException(name, map, "a dictionary");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) value;
    return result;
  }

  // Builder operations.

  public static void addString(Map<String, Object> map, String name, String value) {
    addObject(map, name, CloudObject.forString(value));
  }

  public static void addBoolean(Map<String, Object> map, String name, boolean value) {
    addObject(map, name, CloudObject.forBoolean(value));
  }

  public static void addLong(Map<String, Object> map, String name, long value) {
    addObject(map, name, CloudObject.forInteger(value));
  }

  public static void addObject(
      Map<String, Object> map, String name, Map<String, Object> value) {
    map.put(name, value);
  }

  public static void addNull(Map<String, Object> map, String name) {
    map.put(name, Data.nullOf(Object.class));
  }

  public static void addLongs(Map<String, Object> map, String name, long... longs) {
    List<Map<String, Object>> elements = new ArrayList<>(longs.length);
    for (Long value : longs) {
      elements.add(CloudObject.forInteger(value));
    }
    map.put(name, elements);
  }

  public static void addList(
      Map<String, Object> map, String name, List<? extends Map<String, Object>> elements) {
    map.put(name, elements);
  }

  public static void addStringList(Map<String, Object> map, String name, List<String> elements) {
    ArrayList<CloudObject> objects = new ArrayList<>(elements.size());
    for (String element : elements) {
      objects.add(CloudObject.forString(element));
    }
    addList(map, name, objects);
  }

  public static <T extends Map<String, Object>> void addList(
      Map<String, Object> map, String name, T[] elements) {
    map.put(name, Arrays.asList(elements));
  }

  public static void addDictionary(
      Map<String, Object> map, String name, Map<String, Object> value) {
    map.put(name, value);
  }

  public static void addDouble(Map<String, Object> map, String name, Double value) {
    addObject(map, name, CloudObject.forFloat(value));
  }

  // Helper methods for a few of the accessor methods.

  private static <T> T getValue(Map<String, Object> map, String name, Class<T> clazz, String type)
      throws Exception {
    @Nullable T result = getValue(map, name, clazz, type, null);
    if (result == null) {
      throw new ParameterNotFoundException(name, map);
    }
    return result;
  }

  @Nullable
  private static <T> T getValue(
      Map<String, Object> map, String name, Class<T> clazz, String type, @Nullable T defaultValue)
      throws Exception {
    @Nullable Object value = map.get(name);
    if (value == null) {
      if (map.containsKey(name)) {
        throw new IncorrectTypeException(name, map, type);
      }
      return defaultValue;
    }
    T result = decodeValue(value, clazz);
    if (result == null) {
      // The value exists, but can't be decoded.
      throw new IncorrectTypeException(name, map, type);
    }
    return result;
  }

  @Nullable
  private static <T> T decodeValue(Object value, Class<T> clazz) {
    try {
      if (value.getClass() == clazz) {
        // decodeValue() is only called for final classes; if the class matches,
        // it's safe to just return the value, and if it doesn't match, decoding
        // is needed.
        return clazz.cast(value);
      }
      if (!(value instanceof Map)) {
        return null;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      @Nullable String typeName = (String) map.get(PropertyNames.OBJECT_TYPE_NAME);
      if (typeName == null) {
        return null;
      }
      @Nullable CloudKnownType knownType = CloudKnownType.forUri(typeName);
      if (knownType == null) {
        return null;
      }
      @Nullable Object scalar = map.get(PropertyNames.SCALAR_FIELD_NAME);
      if (scalar == null) {
        return null;
      }
      return knownType.parse(scalar, clazz);
    } catch (ClassCastException e) {
      // If any class cast fails during decoding, the value's not decodable.
      return null;
    }
  }

  private static final class ParameterNotFoundException extends Exception {
    public ParameterNotFoundException(String name, Map<String, Object> map) {
      super("didn't find required parameter " + name + " in " + map);
    }
  }

  private static final class IncorrectTypeException extends Exception {
    public IncorrectTypeException(String name, Map<String, Object> map, String type) {
      super("required parameter " + name + " in " + map + " not " + type);
    }
  }
}
