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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.lang.reflect.Method;

/**
 * For internal use. Specification for an option defined in a {@link PipelineOptions} interface.
 */
class PipelineOptionSpec {
  private final Class<? extends PipelineOptions> clazz;
  private final String name;
  private final Method getter;

  static PipelineOptionSpec of(Class<? extends PipelineOptions> clazz, String name, Method getter) {
    return new PipelineOptionSpec(clazz, name, getter);
  }

  private PipelineOptionSpec(Class<? extends PipelineOptions> clazz, String name, Method getter) {
    this.clazz = clazz;
    this.name = name;
    this.getter = getter;
  }

  /**
   * The {@link PipelineOptions} interface which defines this {@link PipelineOptionSpec}.
   */
  Class<? extends PipelineOptions> getDefiningInterface() {
    return clazz;
  }

  /**
   * Name of the property.
   */
  String getName() {
    return name;
  }

  /**
   * The getter method for this property.
   */
  Method getGetterMethod() {
    return getter;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("definingInterface", getDefiningInterface())
        .add("name", getName())
        .add("getterMethod", getGetterMethod())
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getDefiningInterface(), getName(), getGetterMethod());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PipelineOptionSpec)) {
      return false;
    }

    PipelineOptionSpec that = (PipelineOptionSpec) obj;
    return Objects.equal(this.getDefiningInterface(), that.getDefiningInterface())
        && Objects.equal(this.getName(), that.getName())
        && Objects.equal(this.getGetterMethod(), that.getGetterMethod());
  }
}
