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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ticker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;

final class LimitingTopicBacklogReader implements TopicBacklogReader {
  private final TopicBacklogReader underlying;
  private final LoadingCache<String, ComputeMessageStatsResponse> backlogCache;

  @GuardedBy("this")
  @Nullable
  private Offset currentRequestOffset = null;

  @SuppressWarnings("method.invocation")
  LimitingTopicBacklogReader(TopicBacklogReader underlying, Ticker ticker) {
    this.underlying = underlying;
    backlogCache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .maximumSize(1)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .refreshAfterWrite(10, TimeUnit.SECONDS)
            .build(
                new CacheLoader<String, ComputeMessageStatsResponse>() {
                  @Override
                  public ComputeMessageStatsResponse load(String val) {
                    return loadFromUnderlying();
                  }
                });
  }

  @SuppressWarnings("argument")
  private synchronized ComputeMessageStatsResponse loadFromUnderlying() {
    return underlying.computeMessageStats(checkNotNull(currentRequestOffset));
  }

  @Override
  public synchronized ComputeMessageStatsResponse computeMessageStats(Offset offset)
      throws ApiException {
    currentRequestOffset = offset;
    try {
      // There is only a single entry in the cache.
      return backlogCache.get("cache");
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  @Override
  public void close() throws Exception {
    underlying.close();
  }
}
