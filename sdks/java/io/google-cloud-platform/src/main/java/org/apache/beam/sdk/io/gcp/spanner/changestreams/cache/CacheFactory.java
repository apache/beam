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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.cache;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

public class CacheFactory implements Serializable {

  private static final long serialVersionUID = -8722905670370252723L;
  private static final Map<Long, WatermarkCache> WATERMARK_CACHE = new ConcurrentHashMap<>();
  private static final AtomicLong CACHE_ID = new AtomicLong();

  // The unique id for the cache of this CacheFactory. This guarantees that if the CacheFactory is
  // serialized / deserialized it will get the same instance of the factory.
  private final long cacheId = CACHE_ID.getAndIncrement();
  private final DaoFactory daoFactory;
  private final Duration refreshRate;

  public CacheFactory(DaoFactory daoFactory, Duration watermarkRefreshRate) {
    this.daoFactory = daoFactory;
    this.refreshRate = watermarkRefreshRate;
  }

  public WatermarkCache getWatermarkCache() {
    return WATERMARK_CACHE.computeIfAbsent(
        cacheId,
        key ->
            refreshRate.getMillis() == 0
                ? new NoOpWatermarkCache(daoFactory.getPartitionMetadataDao())
                : new AsyncWatermarkCache(daoFactory.getPartitionMetadataDao(), refreshRate));
  }

  @VisibleForTesting
  long getCacheId() {
    return cacheId;
  }
}
