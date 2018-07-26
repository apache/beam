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

package org.apache.beam.sdk.values.reflect;

import static org.apache.beam.sdk.values.reflect.ReflectionUtils.tryStripGetPrefix;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Implementation of {@link FieldValueGetter} backed by relfection-based getter invocation, as
 * opposed to a code-generated version produced by {@link GeneratedGetterFactory}.
 */
class ReflectionGetter implements FieldValueGetter {
  private String name;
  private Class type;
  private transient Method getter;

  ReflectionGetter(Method getter) {
    this.getter = getter;
    this.name = tryStripGetPrefix(getter);
    this.type = getter.getReturnType();
  }

  @Override
  public Object get(Object object) {
    try {
      return getter.invoke(object);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Unable to invoke " + getter, e);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Class type() {
    return type;
  }
}
