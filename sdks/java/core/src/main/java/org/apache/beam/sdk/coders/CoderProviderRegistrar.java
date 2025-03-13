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
package org.apache.beam.sdk.coders;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.ServiceLoader;

/**
 * {@link Coder} creators have the ability to automatically have their {@link Coder coders}
 * registered with this SDK by creating a {@link ServiceLoader} entry and a concrete implementation
 * of this interface.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as {@link
 * AutoService} to generate the necessary META-INF files automatically.
 */
public interface CoderProviderRegistrar {
  /**
   * Returns a list of {@link CoderProvider coder providers} which will be registered by default
   * within each {@link CoderRegistry coder registry} instance.
   *
   * <p>See {@link CoderProviders} for convenience methods to construct a {@link CoderProvider}.
   */
  List<CoderProvider> getCoderProviders();
}
