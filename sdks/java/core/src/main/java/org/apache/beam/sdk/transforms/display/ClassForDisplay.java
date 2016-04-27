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
package org.apache.beam.sdk.transforms.display;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

/**
 * Display data representing a Java class.
 *
 * <p>Java classes can be registered as display data via
 * {@link DisplayData.Builder#item(String, ClassForDisplay)}. {@link ClassForDisplay} is
 * serializable, unlike {@link Class} which can fail to serialize for Java 8 lambda functions.
 */
public class ClassForDisplay implements Serializable {
  private final String simpleName;
  private final String name;

  private ClassForDisplay(Class<?> clazz) {
    name = clazz.getName();
    simpleName = clazz.getSimpleName();
  }

  /**
   * Create a {@link ClassForDisplay} instance representing the specified class.
   */
  public static ClassForDisplay of(Class<?> clazz) {
    checkNotNull(clazz, "clazz argument cannot be null");
    return new ClassForDisplay(clazz);
  }

  /**
   * Create a {@link ClassForDisplay} from the class of the specified object instance.
   */
  public static ClassForDisplay fromInstance(Object obj) {
    checkNotNull(obj, "obj argument instance cannot be null");
    return new ClassForDisplay(obj.getClass());
  }

  /**
   * Retrieve the fully-qualified name of the class.
   *
   * @see Class#getName()
   */
  public String getName() {
    return name;
  }

  /**
   * Retrieve a simple representation of the class name.
   *
   * @see Class#getSimpleName()
   */
  public String getSimpleName() {
    return simpleName;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ClassForDisplay) {
      ClassForDisplay that = (ClassForDisplay) obj;
      return Objects.equals(this.name, that.name);
    }

    return false;
  }
}
