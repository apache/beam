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

import com.google.auto.value.AutoValue;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.reflect.Method;

/**
 * For internal use. Specification for an option defined in a {@link PipelineOptions} interface.
 */
@AutoValue
abstract class PipelineOptionSpec {
  static PipelineOptionSpec of(Class<? extends PipelineOptions> clazz, String name, Method getter) {
    return new AutoValue_PipelineOptionSpec(clazz, name, getter);
  }

  /**
   * The {@link PipelineOptions} interface which defines this {@link PipelineOptionSpec}.
   */
  abstract Class<? extends PipelineOptions> getDefiningInterface();

  /**
   * Name of the property.
   */
  abstract String getName();

  /**
   * The getter method for this property.
   */
  abstract Method getGetterMethod();

  /**
   * Whether the option should be serialized. Uses the {@link JsonIgnore} annotation.
   */
  boolean shouldSerialize() {
    return !getGetterMethod().isAnnotationPresent(JsonIgnore.class);
  }
}
