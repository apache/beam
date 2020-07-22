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
package org.apache.beam.runners.dataflow.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A utility for manipulating well-known cloud types. */
@SuppressWarnings("ImmutableEnumChecker")
enum CloudKnownType {
  TEXT("http://schema.org/Text", String.class) {
    @Override
    public <T> T parse(Object value, Class<T> clazz) {
      return clazz.cast(value);
    }
  },
  BOOLEAN("http://schema.org/Boolean", Boolean.class) {
    @Override
    public <T> T parse(Object value, Class<T> clazz) {
      return clazz.cast(value);
    }
  },
  INTEGER("http://schema.org/Integer", Long.class, Integer.class) {
    @Override
    public <T> T parse(Object value, Class<T> clazz) {
      Object result = null;
      if (value.getClass() == clazz) {
        result = value;
      } else if (clazz == Long.class) {
        if (value instanceof Integer) {
          result = ((Integer) value).longValue();
        } else if (value instanceof String) {
          result = Long.valueOf((String) value);
        }
      } else if (clazz == Integer.class) {
        if (value instanceof Long) {
          result = ((Long) value).intValue();
        } else if (value instanceof String) {
          result = Integer.valueOf((String) value);
        }
      }
      return clazz.cast(result);
    }
  },
  FLOAT("http://schema.org/Float", Double.class, Float.class) {
    @Override
    public <T> T parse(Object value, Class<T> clazz) {
      Object result = null;
      if (value.getClass() == clazz) {
        result = value;
      } else if (clazz == Double.class) {
        if (value instanceof Float) {
          result = ((Float) value).doubleValue();
        } else if (value instanceof String) {
          result = Double.valueOf((String) value);
        }
      } else if (clazz == Float.class) {
        if (value instanceof Double) {
          result = ((Double) value).floatValue();
        } else if (value instanceof String) {
          result = Float.valueOf((String) value);
        }
      }
      return clazz.cast(result);
    }
  };

  private final String uri;
  private final ImmutableList<Class<?>> classes;

  CloudKnownType(String uri, Class<?>... classes) {
    this.uri = uri;
    this.classes = ImmutableList.copyOf(classes);
  }

  public String getUri() {
    return uri;
  }

  public abstract <T> T parse(Object value, Class<T> clazz);

  public Class<?> defaultClass() {
    return classes.get(0);
  }

  private static final Map<String, CloudKnownType> typesByUri =
      Collections.unmodifiableMap(buildTypesByUri());

  private static Map<String, CloudKnownType> buildTypesByUri() {
    Map<String, CloudKnownType> result = new HashMap<>();
    for (CloudKnownType ty : CloudKnownType.values()) {
      result.put(ty.getUri(), ty);
    }
    return result;
  }

  public static @Nullable CloudKnownType forUri(@Nullable String uri) {
    if (uri == null) {
      return null;
    }
    return typesByUri.get(uri);
  }

  private static final Map<Class<?>, CloudKnownType> typesByClass =
      Collections.unmodifiableMap(buildTypesByClass());

  private static Map<Class<?>, CloudKnownType> buildTypesByClass() {
    Map<Class<?>, CloudKnownType> result = new HashMap<>();
    for (CloudKnownType ty : CloudKnownType.values()) {
      for (Class<?> clazz : ty.classes) {
        result.put(clazz, ty);
      }
    }
    return result;
  }

  public static @Nullable CloudKnownType forClass(Class<?> clazz) {
    return typesByClass.get(clazz);
  }
}
