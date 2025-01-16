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

import com.google.cloud.Timestamp;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;

/**
 * Asynchronously compute the earliest partition watermark and stores it in memory. The value will
 * be recomputed periodically, as configured by the refresh rate.
 *
 * <p>On every period, we will call {@link PartitionMetadataDao#getUnfinishedMinWatermark()} to
 * refresh the value.
 */
public class AsyncWatermarkCache implements WatermarkCache {

  private static final String THREAD_NAME_FORMAT = "watermark_loading_thread_%d";
  private static final Object MIN_WATERMARK_KEY = new Object();
  private final LoadingCache<Object, Optional<Timestamp>> cache;

  public AsyncWatermarkCache(PartitionMetadataDao dao, Duration refreshRate) {
    this.cache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(java.time.Duration.ofMillis(refreshRate.getMillis()))
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(key -> Optional.ofNullable(dao.getUnfinishedMinWatermark())),
                    Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_FORMAT).build())));
  }

  @Override
  public @Nullable Timestamp getUnfinishedMinWatermark() {
    try {
      return cache.get(MIN_WATERMARK_KEY).orElse(null);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
