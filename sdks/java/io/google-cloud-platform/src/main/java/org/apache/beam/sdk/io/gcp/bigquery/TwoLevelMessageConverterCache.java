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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A cache for {@link MessageConverter} objects.
 *
 * <p>There is an instance-level cache which we try to use to avoid hashing the entire operation
 * name. However since this object is stored in DoFns and many DoFns share the same
 * MessageConverters, we also store a static cache keyed by operation name.
 */
class TwoLevelMessageConverterCache<DestinationT extends @NonNull Object, ElementT>
    implements Serializable {
  final String operationName;

  TwoLevelMessageConverterCache(String operationName) {
    this.operationName = operationName;
  }

  // Cache MessageConverters since creating them can be expensive. Cache is keyed by transform name
  // and the destination.
  private static final Cache<KV<String, ?>, MessageConverter<?>> CACHED_MESSAGE_CONVERTERS =
      CacheBuilder.newBuilder().expireAfterAccess(java.time.Duration.ofMinutes(15)).build();

  // Keep an instance-level cache of MessageConverter objects. This allows us to avoid hashing the
  // entire operation name
  // on every element. Since there will be multiple DoFn instances (and they may periodically be
  // recreated), we
  // still need the static cache to allow reuse.
  private final Cache<DestinationT, MessageConverter<ElementT>> localMessageConverters =
      CacheBuilder.newBuilder().expireAfterAccess(java.time.Duration.ofMinutes(15)).build();

  static void clear() {
    CACHED_MESSAGE_CONVERTERS.invalidateAll();
  }

  public MessageConverter<ElementT> get(
      DestinationT destination,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      DatasetService datasetService)
      throws Exception {
    // Lookup first in the local cache, and fall back to the static cache if necessary.
    return localMessageConverters.get(
        destination,
        () ->
            (MessageConverter<ElementT>)
                CACHED_MESSAGE_CONVERTERS.get(
                    KV.of(operationName, destination),
                    () -> dynamicDestinations.getMessageConverter(destination, datasetService)));
  }
}
