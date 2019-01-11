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
package org.apache.beam.sdk.util;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A collection of static methods for manipulating value representations
 * transfered via the Dataflow API.
 */
public final class Values {
  private Values() {}  // Non-instantiable

  public static Boolean asBoolean(Object value) throws ClassCastException {
    @Nullable Boolean knownResult = checkKnownValue(CloudKnownType.BOOLEAN, value, Boolean.class);
    if (knownResult != null) {
      return knownResult;
    }
    return Boolean.class.cast(value);
  }

  public static Double asDouble(Object value) throws ClassCastException {
    @Nullable Double knownResult = checkKnownValue(CloudKnownType.FLOAT, value, Double.class);
    if (knownResult != null) {
      return knownResult;
    }
    if (value instanceof Double) {
      return (Double) value;
    }
    return ((Float) value).doubleValue();
  }

  public static Long asLong(Object value) throws ClassCastException {
    @Nullable Long knownResult = checkKnownValue(CloudKnownType.INTEGER, value, Long.class);
    if (knownResult != null) {
      return knownResult;
    }
    if (value instanceof Long) {
      return (Long) value;
    }
    return ((Integer) value).longValue();
  }

  public static String asString(Object value) throws ClassCastException {
    @Nullable String knownResult = checkKnownValue(CloudKnownType.TEXT, value, String.class);
    if (knownResult != null) {
      return knownResult;
    }
    return String.class.cast(value);
  }

  @Nullable
  private static <T> T checkKnownValue(CloudKnownType type, Object value, Class<T> clazz) {
    if (!(value instanceof Map)) {
      return null;
    }
    Map<String, Object> map = (Map<String, Object>) value;
    @Nullable String typeName = (String) map.get(PropertyNames.OBJECT_TYPE_NAME);
    if (typeName == null) {
      return null;
    }
    @Nullable CloudKnownType knownType = CloudKnownType.forUri(typeName);
    if (knownType == null || knownType != type) {
      return null;
    }
    @Nullable Object scalar = map.get(PropertyNames.SCALAR_FIELD_NAME);
    if (scalar == null) {
      return null;
    }
    return knownType.parse(scalar, clazz);
  }
}
