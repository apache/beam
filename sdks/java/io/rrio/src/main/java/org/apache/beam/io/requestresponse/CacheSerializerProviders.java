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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import org.apache.beam.sdk.schemas.io.Providers;

/**
 * Provides the {@link CacheSerializer} using its associated {@link CacheSerializerProvider}. See
 * {@link CacheSerializerProvider} for further information on the requirements to create and
 * register a new {@link CacheSerializer} with the {@link CacheSerializerProviders}.
 */
public final class CacheSerializerProviders {
  private CacheSerializerProviders() {}

  private static final Map<String, CacheSerializerProvider> PROVIDERS =
      Providers.loadProviders(CacheSerializerProvider.class);

  /** Queries the service loader */
  public static <T> CacheSerializer<T> getSerializer(String id, Class<T> clazz) {
    CacheSerializerProvider provider =
        checkStateNotNull(
            PROVIDERS.get(id), "No serializer provider exists with identifier `%s`.", id);

    return provider.getSerializer(clazz);
  }
}
