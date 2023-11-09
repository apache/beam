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
package org.apache.beam.io.requestresponse;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.Providers;

/**
 * Provides a {@link CacheSerializer} using a {@link Providers.Identifyable#identifier}. To register
 * a new {@link CacheSerializer} with {@link CacheSerializerProviders}, one simply needs to add the
 * {@link AutoService} annotation to the {@link CacheSerializer}'s associated {@link
 * CacheSerializerProvider} and assign the {@link CacheSerializerProvider} class to {@link
 * AutoService#value} See {@link JsonCacheSerializerProvider} for an example.
 */
public interface CacheSerializerProvider extends Providers.Identifyable {
  Schema getConfigurationSchema();
  <T> CacheSerializer<T> getSerializer(Class<T> clazz);
}
