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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** And implementation of {@link SchemaUserTypeCreator} that uses a Java constructor. */
public class SchemaUserTypeConstructorCreator implements SchemaUserTypeCreator {
  private final Class<?> clazz;
  private final transient Constructor<?> constructor;

  public SchemaUserTypeConstructorCreator(Class<?> clazz, Constructor<?> constructor) {
    this.clazz = clazz;
    this.constructor = checkNotNull(constructor);
  }

  @Override
  public Object create(Object... params) {
    checkNotNull(constructor);
    try {
      return constructor.newInstance(params);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Could not instantiate object " + clazz, e);
    }
  }
}
