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

/**
 * An interface used with the {@link Default.InstanceFactory} annotation to specify the class that
 * will be an instance factory to produce default values for a given getter on {@link
 * PipelineOptions}. When a property on a {@link PipelineOptions} is fetched, and is currently
 * unset, the default value factory will be instantiated and invoked.
 *
 * <p>Care must be taken to not produce an infinite loop when accessing other fields on the {@link
 * PipelineOptions} object.
 *
 * @param <T> The type of object this factory produces.
 */
public interface DefaultValueFactory<T> {
  /**
   * Creates a default value for a getter marked with {@link Default.InstanceFactory}.
   *
   * @param options The current pipeline options.
   * @return The default value to be used for the annotated getter.
   */
  T create(PipelineOptions options);
}
